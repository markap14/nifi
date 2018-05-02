/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pcap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.StandardSchemaIdentifier;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.PcapNetworkInterface.PromiscuousMode;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.EthernetPacket.EthernetHeader;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.IpV4Packet.IpV4Header;
import org.pcap4j.packet.IpV4Packet.IpV4Option;
import org.pcap4j.packet.IpV6Packet;
import org.pcap4j.packet.IpV6Packet.IpV6Header;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.TcpPacket.TcpHeader;
import org.pcap4j.packet.TcpPacket.TcpOption;
import org.pcap4j.packet.UdpPacket;
import org.pcap4j.packet.UdpPacket.UdpHeader;
import org.pcap4j.packet.namednumber.EtherType;
import org.pcap4j.packet.namednumber.IpNumber;
import org.pcap4j.packet.namednumber.IpV4OptionType.IpV4OptionClass;
import org.pcap4j.packet.namednumber.Oui;
import org.pcap4j.util.MacAddress;

@Tags({"pcap", "packet", "record", "metadata"})
@CapabilityDescription("Captures packets from a configured network interface and outputs the packets' metadata as records. "
    + "This processor will typically require that the user running NiFi have elevated Operating System permissions.")
public class CapturePacketRecord extends AbstractProcessor {
    private static final String SCHEMA_FILENAME = "pcap.avsc";
    static final RecordSchema RECORD_SCHEMA;
    private static final RecordSchema ETHERNET_HEADER_SCHEMA;
    private static final RecordSchema MAC_ADDRESS_SCHEMA;
    private static final RecordSchema IPV4_SCHEMA;
    private static final RecordSchema IPV6_SCHEMA;
    private static final RecordSchema TCP_SCHEMA;
    private static final RecordSchema UDP_SCHEMA;

    static {
        RECORD_SCHEMA = createSchema();
        ETHERNET_HEADER_SCHEMA = ((RecordDataType) RECORD_SCHEMA.getField("header").get().getDataType()).getChildSchema();
        MAC_ADDRESS_SCHEMA = ((RecordDataType) ETHERNET_HEADER_SCHEMA.getField("srcAddr").get().getDataType()).getChildSchema();

        final List<DataType> ipDataTypes = ((ChoiceDataType) RECORD_SCHEMA.getField("ip").get().getDataType()).getPossibleSubTypes();
        IPV4_SCHEMA = ((RecordDataType) ipDataTypes.get(0)).getChildSchema();
        IPV6_SCHEMA = ((RecordDataType) ipDataTypes.get(1)).getChildSchema();

        TCP_SCHEMA = ((RecordDataType) IPV4_SCHEMA.getField("tcp").get().getDataType()).getChildSchema();
        UDP_SCHEMA = ((RecordDataType) IPV4_SCHEMA.getField("udp").get().getDataType()).getChildSchema();
    }

    static final PropertyDescriptor ADDRESS = new PropertyDescriptor.Builder()
        .name("address")
        .displayName("Network Address")
        .description("The network address to listen on for packets. If not specified, will use whatever address is returned by 'hostname'")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(false)
        .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Record Writer that will be used to write out the metadata of the packets captured")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor MAX_RECORDS = new PropertyDescriptor.Builder()
        .name("max-records")
        .displayName("Maximum Records per FlowFile")
        .description("The maximum number of Records to include in a single FlowFile.")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(false)
        .build();
    static final PropertyDescriptor MAX_FLOWFILE_SIZE = new PropertyDescriptor.Builder()
        .name("max-flowfile-size")
        .displayName("Maximum FlowFile Size")
        .description("The maximum size that a FlowFile can reach before a new FlowFile is created and the previous one emitted from the processor")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .defaultValue("100 KB")
        .required(false)
        .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Packets that are captured are routed to this relationship.")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private volatile PcapHandle pcapHandle;
    private volatile String address;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ADDRESS);
        properties.add(RECORD_WRITER);
        properties.add(MAX_RECORDS);
        properties.add(MAX_FLOWFILE_SIZE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void setup(final ProcessContext context) throws UnknownHostException, PcapNativeException {
        address = context.getProperty(ADDRESS).evaluateAttributeExpressions().getValue();
        if (address == null || address.trim().isEmpty()) {
            address = InetAddress.getLocalHost().getHostAddress();
        }

        final InetAddress addr = InetAddress.getByName(address);
        final PcapNetworkInterface nif = Pcaps.getDevByAddress(addr);
        if (nif == null) {
            throw new UnknownHostException("Unable to find Network Device for address " + addr);
        }

        final int snapLen = 65536;
        final PromiscuousMode mode = PromiscuousMode.PROMISCUOUS;
        final int timeout = 90;

        pcapHandle = nif.openLive(snapLen, mode, timeout);
    }

    @OnStopped
    public void tearDown() {
        if (pcapHandle != null) {
            pcapHandle.close();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        Packet packet = getPacket();
        if (packet == null) {
            context.yield();
            return;
        }

        FlowFile flowFile = session.create();
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        Integer maxRecords = context.getProperty(MAX_RECORDS).evaluateAttributeExpressions().asInteger();
        if (maxRecords == null) {
            maxRecords = Integer.MAX_VALUE;
        }

        Double maxSize = context.getProperty(MAX_FLOWFILE_SIZE).evaluateAttributeExpressions().asDataSize(DataUnit.B);
        if (maxSize == null) {
            maxSize = Double.MAX_VALUE;
        }

        int recordCount = 1;
        final Map<String, String> attributes = new HashMap<>();
        try (final OutputStream flowFileOut = session.write(flowFile);
            final ByteCountingOutputStream bcos = new ByteCountingOutputStream(flowFileOut);
            final RecordSetWriter writer = writerFactory.createWriter(getLogger(), RECORD_SCHEMA, bcos)) {

            writer.beginRecordSet();

            while (packet != null && recordCount < maxRecords && bcos.getBytesWritten() < maxSize) {
                final Record record = createRecord(packet);
                writer.write(record);

                packet = getPacket();

                if (packet != null) {
                    recordCount++;
                }
            }

            final WriteResult result = writer.finishRecordSet();
            attributes.putAll(result.getAttributes());
            attributes.put("record.count", String.valueOf(result.getRecordCount()));
            attributes.put("network.capture.address", address);
            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
        } catch (IOException | SchemaNotFoundException e) {
            getLogger().error("Failed to serialize packets from network", e);
            session.rollback();
            return;
        }

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, SUCCESS);
        session.getProvenanceReporter().create(flowFile);
        session.adjustCounter("Packets Captured", recordCount, false);
    }


    /**
     * Creates a new Record from a given Packet
     *
     * @param packet the packet to convert
     * @return a Record that represents the given packet
     */
    final Record createRecord(final Packet packet) {
        if (packet instanceof EthernetPacket) {
            return createRecord((EthernetPacket) packet);
        } else {
            getLogger().debug("Received Packet of type {} but expected EthernetPacket. Will ignore this packet.", new Object[] {packet.getClass().getSimpleName()});
            return null;
        }
    }

    private Record createRecord(final EthernetPacket ethernetPacket) {
        final EthernetHeader header = ethernetPacket.getHeader();
        final Record headerRecord = new MapRecord(ETHERNET_HEADER_SCHEMA, new HashMap<>());

        final EtherType etherType = header.getType();
        headerRecord.setValue("etherTypeName", etherType.name());
        headerRecord.setValue("etherTypeNumber", etherType.value());

        headerRecord.setValue("srcAddr", createRecord(header.getSrcAddr()));
        headerRecord.setValue("dstAddr", createRecord(header.getDstAddr()));

        final Record record = new MapRecord(RECORD_SCHEMA, new HashMap<>());
        record.setValue("header", headerRecord);

        final Packet innerPacket = ethernetPacket.getPayload();
        if (innerPacket instanceof IpV4Packet) {
            record.setValue("ip", createRecord((IpV4Packet) innerPacket));
            getLogger().info("Received IPv4 Packet");
        } else if (innerPacket instanceof IpV6Packet) {
            record.setValue("ip", createRecord((IpV6Packet) innerPacket));
            getLogger().info("Received IPv6 Packet");
        } else {
            getLogger().debug("Received Ethernet Packet that wrapped a packet of type {}, but that type is not supported", new Object[] {innerPacket.getClass().getSimpleName()});
        }

        return record;
    }

    private Record createRecord(final IpV4Packet ipv4Packet) {
        final RecordSchema headerSchema = ((RecordDataType) IPV4_SCHEMA.getField("header").get().getDataType()).getChildSchema();
        final Record headerRecord = new MapRecord(headerSchema, new HashMap<>());

        final RecordSchema ipv4AddressSchema = ((RecordDataType) headerSchema.getField("dstAddr").get().getDataType()).getChildSchema();

        final IpV4Header header = ipv4Packet.getHeader();
        headerRecord.setValue("dontFragmentFlag", header.getDontFragmentFlag());
        headerRecord.setValue("dstAddr", createRecord(header.getDstAddr(), ipv4AddressSchema));
        headerRecord.setValue("fragmentOffset", header.getFragmentOffset());
        headerRecord.setValue("identification", header.getIdentificationAsInt());
        headerRecord.setValue("ihl", header.getIhlAsInt());
        headerRecord.setValue("moreFragmentFlag", header.getMoreFragmentFlag());

        final List<IpV4Option> optionList = header.getOptions();
        final List<Record> optionRecords = new ArrayList<>();
        if (optionList != null) {
            final DataType optionsRecordType = ((ArrayDataType) headerSchema.getField("options").get().getDataType()).getElementType();
            final RecordSchema optionSchema = ((RecordDataType) optionsRecordType).getChildSchema();

            for (final IpV4Option option : optionList) {
                final IpV4OptionClass optionClass = option.getType().getOptionClass();
                final String name = optionClass.name();
                final int number = optionClass.getValue();

                final Record optionRecord = new MapRecord(optionSchema, new HashMap<>());
                optionRecord.setValue("name", name);
                optionRecord.setValue("number", number);
                optionRecords.add(optionRecord);
            }
        }
        headerRecord.setValue("options", optionRecords.toArray());

        final IpNumber protocol = header.getProtocol();
        if (protocol != null) {
            headerRecord.setValue("protocolName", protocol.name());
            headerRecord.setValue("protocolNumber", protocol.value());
        }

        headerRecord.setValue("reservedFlag", header.getReservedFlag());
        headerRecord.setValue("srcAddr", createRecord(header.getSrcAddr(), ipv4AddressSchema));
        headerRecord.setValue("tos", header.getTos() == null ? null : header.getTos().value());
        headerRecord.setValue("totalLength", header.getTotalLengthAsInt());
        headerRecord.setValue("ttl", header.getTtlAsInt());
        headerRecord.setValue("validChecksum", header.hasValidChecksum(false));

        final Record record = new MapRecord(IPV4_SCHEMA, new HashMap<>());
        record.setValue("header", headerRecord);

        final Packet packet = ipv4Packet.getPayload();
        if (packet == null) {
            getLogger().debug("IPv4 Packet has no payload");
        } else if (packet instanceof TcpPacket) {
            record.setValue("tcp", createRecord((TcpPacket) packet));
        } else if (packet instanceof UdpPacket) {
            record.setValue("udp", createRecord((UdpPacket) packet));
        } else {
            getLogger().debug("IPv4 Packet has payload of type {}, which is not supported at this time", new Object[] {packet.getClass().getSimpleName()});
        }

        return record;
    }

    private Record createRecord(final InetAddress address, final RecordSchema schema) {
        if (address == null) {
            return null;
        }

        final Record record = new MapRecord(schema, new HashMap<>());
        record.setValue("address", address.getHostAddress());
        record.setValue("hostname", address.getHostName());
        record.setValue("anyLocalAddress", address.isAnyLocalAddress());
        record.setValue("linkLocalAddress", address.isLinkLocalAddress());
        record.setValue("loopbackAddress", address.isLoopbackAddress());
        record.setValue("mcGlobal", address.isMCGlobal());
        record.setValue("mcLinkLocal", address.isMCLinkLocal());
        record.setValue("mcNodeLocal", address.isMCNodeLocal());
        record.setValue("mcOrgLocal", address.isMCOrgLocal());
        record.setValue("mcSiteLocal", address.isMCSiteLocal());
        record.setValue("multicastAddress", address.isMulticastAddress());
        record.setValue("siteLocalAddress", address.isSiteLocalAddress());
        return record;
    }

    private Record createRecord(final Inet6Address address, final RecordSchema schema) {
        if (address == null) {
            return null;
        }

        final Record record = createRecord((InetAddress) address, schema);
        record.setValue("ipV4Compatiable", address.isIPv4CompatibleAddress());
        return record;
    }

    private Record createRecord(final TcpPacket packet) {
        if (packet == null) {
            return null;
        }

        final Record record = new MapRecord(TCP_SCHEMA, new HashMap<>());
        final TcpHeader header = packet.getHeader();

        record.setValue("ack", header.getAck());
        record.setValue("acknowledgementNumber", header.getAcknowledgmentNumberAsLong());
        record.setValue("fin", header.getFin());
        record.setValue("psh", header.getPsh());
        record.setValue("rst", header.getRst());
        record.setValue("sequenceNumber", header.getSequenceNumberAsLong());
        record.setValue("syn", header.getSyn());
        record.setValue("urg", header.getUrg());
        record.setValue("urgentPointer", header.getUrgentPointerAsInt());
        record.setValue("window", header.getWindowAsInt());
        record.setValue("reserved", header.getReserved());
        record.setValue("srcPort", header.getSrcPort().valueAsInt());
        record.setValue("dstPort", header.getDstPort().valueAsInt());

        final List<TcpOption> options = header.getOptions();
        if (options != null) {
            final ArrayDataType optionsArrayType = ((ArrayDataType) TCP_SCHEMA.getField("options").get().getDataType());
            final RecordSchema optionSchema = ((RecordDataType) optionsArrayType.getElementType()).getChildSchema();

            final List<Record> optionRecords = new ArrayList<>();
            for (final TcpOption option : options) {
                final Record optionRecord = new MapRecord(optionSchema, new HashMap<>());
                optionRecord.setValue("name", option.getKind().name());
                optionRecord.setValue("number", option.getKind().value());
                optionRecords.add(optionRecord);
            }

            record.setValue("options", optionRecords.toArray());
        }

        return record;
    }

    private Record createRecord(final UdpPacket packet) {
        if (packet == null) {
            return null;
        }

        final Record record = new MapRecord(UDP_SCHEMA, new HashMap<>());
        final UdpHeader header = packet.getHeader();

        record.setValue("srcPort", header.getSrcPort().valueAsInt());
        record.setValue("dstPort", header.getDstPort().valueAsInt());
        record.setValue("length", header.getLengthAsInt());

        return record;
    }

    private Record createRecord(final IpV6Packet ipv6Packet) {
        final RecordSchema headerSchema = ((RecordDataType) IPV6_SCHEMA.getField("header").get().getDataType()).getChildSchema();
        final Record headerRecord = new MapRecord(headerSchema, new HashMap<>());
        final IpV6Header header = ipv6Packet.getHeader();

        final RecordSchema ipv6AddressSchema = ((RecordDataType) headerSchema.getField("dstAddr").get().getDataType()).getChildSchema();

        headerRecord.setValue("dstAddr", createRecord(header.getDstAddr(), ipv6AddressSchema));
        headerRecord.setValue("flowLabel", header.getFlowLabel().value());
        headerRecord.setValue("hopLimit", header.getHopLimitAsInt());
        headerRecord.setValue("payloadLength", header.getPayloadLengthAsInt());
        headerRecord.setValue("protocolName", header.getProtocol().name());
        headerRecord.setValue("protocolNumber", header.getProtocol().value());
        headerRecord.setValue("nextHeaderName", header.getNextHeader().name());
        headerRecord.setValue("nextHeaderNumber", header.getNextHeader().value());
        headerRecord.setValue("srcAddr", createRecord(header.getSrcAddr(), ipv6AddressSchema));
        headerRecord.setValue("trafficClass", header.getTrafficClass() == null ? null : header.getTrafficClass().value());

        final Record record = new MapRecord(IPV6_SCHEMA, new HashMap<>());
        record.setValue("header", headerRecord);

        final Packet packet = ipv6Packet.getPayload();
        if (packet == null) {
            getLogger().debug("IPv6 Packet has no payload");
        } else if (packet instanceof TcpPacket) {
            record.setValue("tcp", createRecord((TcpPacket) packet));
        } else if (packet instanceof UdpPacket) {
            record.setValue("udp", createRecord((UdpPacket) packet));
        } else {
            getLogger().debug("IPv6 Packet has payload of type {}, which is not supported at this time", new Object[] {packet.getClass().getSimpleName()});
        }

        return record;
    }

    private Record createRecord(final MacAddress macAddress) {
        final Record record = new MapRecord(MAC_ADDRESS_SCHEMA, new HashMap<>());
        record.setValue("globallyUnique", macAddress.isGloballyUnique());
        record.setValue("unicast", macAddress.isUnicast());

        final Oui oui = macAddress.getOui();
        if (oui != null) {
            record.setValue("ouiName", oui.name());
            record.setValue("ouiNumber", oui.value());
        }

        final byte[] addressBytes = macAddress.getAddress();
        if (addressBytes != null) {
            record.setValue("address", Hex.encodeHexString(addressBytes));
        }

        return record;
    }

    protected Packet getPacket() {
        try {
            return pcapHandle.getNextPacket();
        } catch (final Exception e) {
            getLogger().warn("Failed to capture packet from network", e);
            return null;
        }
    }


    protected static RecordSchema createSchema() {
        final String schemaText;
        try (final InputStream in = CapturePacketRecord.class.getClassLoader().getResourceAsStream(SCHEMA_FILENAME);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            StreamUtils.copy(in, baos);
            schemaText = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to read Record Schema for PCAP Data", e);
        }

        final Schema avroSchema = new Schema.Parser().parse(schemaText);
        final SchemaIdentifier schemaId = new StandardSchemaIdentifier.Builder()
            .name("pcap")
            .build();

        return AvroTypeUtil.createSchema(avroSchema, schemaText, schemaId);
    }
}
