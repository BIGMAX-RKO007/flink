package org.example.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.Arrays;
import java.util.List;

public class FlinkList {
    private static final Logger logger = LoggerFactory.getLogger(FlinkList.class);
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Integer> data = Arrays.asList(1, 22, 3);
        logger.warn("data list: {}", data);
        DataStreamSource<Integer> ds = env.fromCollection(data);
        ds.print();
        env.execute();
    }
}
