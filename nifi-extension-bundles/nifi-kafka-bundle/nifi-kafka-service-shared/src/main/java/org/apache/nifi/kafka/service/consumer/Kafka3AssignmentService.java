/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.kafka.service.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.nifi.kafka.service.api.common.PartitionState;
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.kafka.service.api.consumer.PollingSummary;
import org.apache.nifi.kafka.service.api.record.ByteRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class Kafka3AssignmentService implements KafkaConsumerService {
    private final Consumer<byte[], byte[]> consumer;
    private volatile boolean closed = false;

    public Kafka3AssignmentService(final Consumer<byte[], byte[]> consumer, final Collection<String> topicNames) {
        this.consumer = consumer;

        final List<TopicPartition> topicPartitions = new ArrayList<>();
        for (final String topicName : topicNames) {
            final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);
            partitionInfos.forEach(info -> topicPartitions.add(new TopicPartition(info.topic(), info.partition())));
        }

        consumer.assign(topicPartitions);
    }

    @Override
    public void commit(final PollingSummary pollingSummary) {
        throw new UnsupportedOperationException("Commit not supported for assignment-based consumer");
    }

    @Override
    public void rollback() {
        // no-op
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public Iterable<ByteRecord> poll(final Duration maxWaitDuration) {
        final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(maxWaitDuration);
        if (consumerRecords.isEmpty()) {
            return List.of();
        }

        return new RecordIterable(consumerRecords);
    }

    @Override
    public List<PartitionState> getPartitionStates() {
        final Set<TopicPartition> topicPartitions = consumer.assignment();
        return topicPartitions.stream()
            .map(tp -> new PartitionState(tp.topic(), tp.partition()))
            .toList();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;
        consumer.close();
    }
}
