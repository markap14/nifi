/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.kafka.service.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RecordIterable implements Iterable<ByteRecord> {
    private final Iterator<ByteRecord> records;

    public RecordIterable(final Iterable<ConsumerRecord<byte[], byte[]>> consumerRecords) {
        this.records = new RecordIterator(consumerRecords);
    }

    @Override
    public Iterator<ByteRecord> iterator() {
        return records;
    }

    private static class RecordIterator implements Iterator<ByteRecord> {
        private final Iterator<ConsumerRecord<byte[], byte[]>> consumerRecords;

        private RecordIterator(final Iterable<ConsumerRecord<byte[], byte[]>> records) {
            this.consumerRecords = records.iterator();
        }

        @Override
        public boolean hasNext() {
            return consumerRecords.hasNext();
        }

        @Override
        public ByteRecord next() {
            final ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecords.next();
            final List<RecordHeader> recordHeaders = new ArrayList<>();
            consumerRecord.headers().forEach(header -> {
                final RecordHeader recordHeader = new RecordHeader(header.key(), header.value());
                recordHeaders.add(recordHeader);
            });

            // Support Kafka tombstones
            byte[] value = consumerRecord.value();
            if (value == null) {
                value = new byte[0];
            }

            return new ByteRecord(
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.timestamp(),
                recordHeaders,
                consumerRecord.key(),
                value,
                1
            );
        }
    }
}