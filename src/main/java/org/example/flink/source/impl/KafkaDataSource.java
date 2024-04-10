package org.example.flink.source.impl;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.flink.source.DataSource;

import java.util.Properties;

public class KafkaDataSource implements DataSource {
    private final String bootstrapServers;
    private final String topics;
    private final String groupId;

    public KafkaDataSource(String bootstrapServers, String topics, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.topics = topics;
        this.groupId = groupId;
    }

    @Override
    public DataStreamSource getSource(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("group.id", groupId);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");
    }
}
