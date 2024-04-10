package org.example.flink.source.impl;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.source.DataSource;

import java.util.List;

public class CollectionDataSource implements DataSource {
    private final List data;

    public CollectionDataSource(List data) {
        this.data = data;
    }
    @Override
    public DataStreamSource getSource(StreamExecutionEnvironment env) {
        return env.fromCollection(data);
    }
}
