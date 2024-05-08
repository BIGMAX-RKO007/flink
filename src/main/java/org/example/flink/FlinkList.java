package org.example.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.source.DataSource;
import org.example.flink.source.DataSourceFactory;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.Arrays;
import java.util.List;

public class FlinkList {
    //todo 日志对象
    private static final Logger logger = LoggerFactory.getLogger(FlinkList.class);
    //可替换的source数据源
    //"socket", "124.221.143.92", 7777
    //"collection", data
    //String windowsFilePath = "C:/path/to/file.txt";  &&  String linuxFilePath = "/path/to/file.txt";
    public static <T> void main(String[] args) throws Exception {
        //todo 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //模拟list类型
        List<Integer> data = Arrays.asList(1, 22, 3);
        logger.warn("当测试数据是List :"+data);
        String windowsFilePath = "input/word.txt";
        logger.warn("当测试数据是windowsFilePath :"+windowsFilePath);

        String brokers = "127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094";
        String topic = "test001";
        String groupId = "my";
        //todo 数据源
        DataStreamSource<T> DataSource = (DataStreamSource<T>) DataSourceFactory.createDataSource("kafka", brokers,topic,groupId).getSource(env);

        DataSource.print();
        env.execute();
    }
}
