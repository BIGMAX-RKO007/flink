package org.example.flink.source.impl;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.source.DataSource;

public class GeneratorDataSource implements DataSource {
    @Override
    public DataStreamSource getSource(StreamExecutionEnvironment env) {
        return null;
    }
}
