package org.example.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface DataSource {
    DataStreamSource getSource(StreamExecutionEnvironment env);
}
