package org.example.flink.source;

import org.example.flink.source.impl.CollectionDataSource;
import org.example.flink.source.impl.FileDataSource;
import org.example.flink.source.impl.KafkaDataSource;
import org.example.flink.source.impl.SocketDataSource;

import java.util.List;

@SuppressWarnings("unchecked")
public class DataSourceFactory {
    public static DataSource createDataSource(String type, Object... params) {
        if ("collection".equals(type)) {
            // params[0] 应该是 List<T> 类型的数据
            List data = (List) params[0];
            return new CollectionDataSource(data);
        } else if ("socket".equals(type)) {
            // params[0] 应该是 String 类型的 hostname
            // params[1] 应该是 Integer 类型的 port
            return new SocketDataSource((String) params[0], (Integer) params[1]);
        } else if ("file".equals(type)) {
            return new FileDataSource((String) params[0]);
        } else if ("kafka".equals(type)) {
            // 创建 Kafka 数据源
            return new KafkaDataSource((String) params[0], (String) params[1], (String) params[2]);
        } else if ("Generator".equals(type)) {
            // 创建 Generator 数据源
            throw new IllegalArgumentException("Unknown data source type: " + type);
        }else {
            throw new IllegalArgumentException("Unknown data source type: " + type);
        }
    }
}
