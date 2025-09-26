/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

public class ConnectorUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static VersionedExternalFlow loadFlowFromResource(final String resourceName) {
        try (final InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IllegalArgumentException("Resource not found: " + resourceName);
            }

            return OBJECT_MAPPER.readValue(in, VersionedExternalFlow.class);
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to load resource: " + resourceName, e);
        }
    }

    public static Optional<VersionedProcessor> findProcessor(final VersionedProcessGroup group, final Predicate<VersionedProcessor> predicate) {
        final List<VersionedProcessor> processors = findProcessors(group, predicate);
        if (processors.size() == 1) {
            return Optional.of(processors.getFirst());
        }
        return Optional.empty();
    }

    public static List<VersionedProcessor> findProcessors(final VersionedProcessGroup group, final Predicate<VersionedProcessor> predicate) {
        final List<VersionedProcessor> processors = new ArrayList<>();
        findProcessors(group, predicate, processors);
        return processors;
    }

    private static void findProcessors(final VersionedProcessGroup group, final Predicate<VersionedProcessor> predicate, final List<VersionedProcessor> processors) {
        for (final VersionedProcessor processor : group.getProcessors()) {
            if (predicate.test(processor)) {
                processors.add(processor);
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            findProcessors(childGroup, predicate, processors);
        }
    }

    public static ConnectableComponent createConnectableComponent(final VersionedProcessor processor) {
        final ConnectableComponent component = new ConnectableComponent();
        component.setId(processor.getIdentifier());
        component.setName(processor.getName());
        component.setType(ConnectableComponentType.PROCESSOR);
        component.setGroupId(processor.getGroupIdentifier());
        return component;
    }

    public static ConnectableComponent createConnectableComponent(final VersionedPort port) {
        final ConnectableComponent component = new ConnectableComponent();
        component.setId(port.getIdentifier());
        component.setName(port.getName());
        component.setType(port.getComponentType() == ComponentType.INPUT_PORT ? ConnectableComponentType.INPUT_PORT : ConnectableComponentType.OUTPUT_PORT);
        component.setGroupId(port.getGroupIdentifier());
        return component;
    }

    public static void addConnection(final VersionedProcessGroup group, final ConnectableComponent source, final ConnectableComponent destination, final Set<String> relationships) {
        final VersionedConnection connection = new VersionedConnection();
        connection.setSource(source);
        connection.setDestination(destination);
        connection.setSelectedRelationships(relationships);
        connection.setBends(List.of());
        connection.setLabelIndex(0);
        connection.setzIndex(0L);
        connection.setGroupIdentifier(group.getIdentifier());
        connection.setLoadBalanceStrategy("DO_NOT_LOAD_BALANCE");
        connection.setBackPressureDataSizeThreshold("1 GB");
        connection.setBackPressureObjectThreshold(10000L);
        connection.setFlowFileExpiration("0 sec");
        connection.setPrioritizers(new ArrayList<>());
        connection.setComponentType(ComponentType.CONNECTION);

        Set<VersionedConnection> connections = group.getConnections();
        if (connections == null) {
            connections = new HashSet<>();
            group.setConnections(connections);
        }
        connections.add(connection);

        final String uuidSeed = "%s-%s-%s-%s".formatted(group.getIdentifier(), source.getId(), destination.getId(), connections.size());
        final String uuid = UUID.nameUUIDFromBytes(uuidSeed.getBytes(StandardCharsets.UTF_8)).toString();
        connection.setIdentifier(uuid);
    }

    public static List<VersionedConnection> findOutboundConnections(final VersionedProcessGroup group, final VersionedProcessor processor) {
        final VersionedProcessGroup processorGroup = findGroupForProcessor(group, processor);
        if (processorGroup == null) {
            return List.of();
        }

        final List<VersionedConnection> outboundConnections = new ArrayList<>();
        final Set<VersionedConnection> connections = processorGroup.getConnections();
        if (connections == null) {
            return outboundConnections;
        }

        for (final VersionedConnection connection : connections) {
            final ConnectableComponent source = connection.getSource();
            if (Objects.equals(source.getId(), processor.getIdentifier()) && source.getType() == ConnectableComponentType.PROCESSOR) {
                outboundConnections.add(connection);
            }
        }

        return outboundConnections;
    }

    public static VersionedProcessGroup findGroupForProcessor(final VersionedProcessGroup rootGroup, final VersionedProcessor processor) {
        if (rootGroup.getProcessors().contains(processor)) {
            return rootGroup;
        }

        for (final VersionedProcessGroup childGroup : rootGroup.getProcessGroups()) {
            final VersionedProcessGroup foundGroup = findGroupForProcessor(childGroup, processor);
            if (foundGroup != null) {
                return foundGroup;
            }
        }

        return null;
    }

    public static String generateDeterministicUuid(final VersionedProcessGroup group, final ComponentType componentType) {
        final int componentCount = getComponentCount(group, componentType);
        final String uuidSeed = "%s-%s-%d".formatted(group.getIdentifier(), componentType.name(), componentCount);
        return UUID.nameUUIDFromBytes(uuidSeed.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private static int getComponentCount(final VersionedProcessGroup group, final ComponentType componentType) {
        return switch (componentType) {
            case PROCESSOR -> sizeOf(group.getProcessors());
            case INPUT_PORT -> sizeOf(group.getInputPorts());
            case OUTPUT_PORT -> sizeOf(group.getOutputPorts());
            case CONNECTION -> sizeOf(group.getConnections());
            case FUNNEL -> sizeOf(group.getFunnels());
            case LABEL -> sizeOf(group.getLabels());
            case PROCESS_GROUP -> sizeOf(group.getProcessGroups());
            case CONTROLLER_SERVICE -> sizeOf(group.getControllerServices());
            default -> 0;
        };
    }

    private static int sizeOf(final Collection<?> collection) {
        return collection != null ? collection.size() : 0;
    }

    public static VersionedProcessor createProcessor(final VersionedProcessGroup group, final String processorType, final String name, final Position position, final Bundle bundle) {
        final VersionedProcessor processor = new VersionedProcessor();

        // Generate deterministic UUID based on group and component type
        processor.setIdentifier(generateDeterministicUuid(group, ComponentType.PROCESSOR));

        processor.setName(name);
        processor.setType(processorType);
        processor.setPosition(position);
        processor.setBundle(bundle);

        // Set default processor configuration
        processor.setProperties(new HashMap<>());
        processor.setPropertyDescriptors(new HashMap<>());
        processor.setStyle(new HashMap<>());
        processor.setSchedulingPeriod("0 sec");
        processor.setSchedulingStrategy("TIMER_DRIVEN");
        processor.setExecutionNode("ALL");
        processor.setPenaltyDuration("30 sec");
        processor.setYieldDuration("1 sec");
        processor.setBulletinLevel("WARN");
        processor.setRunDurationMillis(25L);
        processor.setConcurrentlySchedulableTaskCount(1);
        processor.setAutoTerminatedRelationships(new HashSet<>());
        processor.setScheduledState(ScheduledState.ENABLED);
        processor.setRetryCount(10);
        processor.setRetriedRelationships(new HashSet<>());
        processor.setBackoffMechanism("PENALIZE_FLOWFILE");
        processor.setMaxBackoffPeriod("10 mins");
        processor.setComponentType(ComponentType.PROCESSOR);
        processor.setGroupIdentifier(group.getIdentifier());

        return processor;
    }
}
