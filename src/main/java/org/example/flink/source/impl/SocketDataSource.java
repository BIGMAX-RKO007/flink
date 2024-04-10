package org.example.flink.source.impl;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.source.DataSource;

public class SocketDataSource implements DataSource {
    private final String hostname;
    private final int port;

    public SocketDataSource(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }
    @Override
    public DataStreamSource getSource(StreamExecutionEnvironment env) {
        return env.socketTextStream(hostname, port);
    }
}
