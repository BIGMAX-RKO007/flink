package org.example.flink.source.impl;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.source.DataSource;

public class FileDataSource implements DataSource {
    private final String filePath;

    public FileDataSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public DataStreamSource getSource(StreamExecutionEnvironment env) {
        // 在这里根据实际需要读取文件数据源
        return env.readTextFile(filePath);
    }
}
