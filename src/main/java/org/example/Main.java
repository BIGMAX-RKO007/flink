package org.example;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {

        System.out.println("Hello world!");
        //kafkatestproducerextracted();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataGeneratorSource<String> dataGeneratorSource =
                new DataGeneratorSource<>(
                        new GeneratorFunction<Long, String>() {
                            @Override
                            public String map(Long value) throws Exception {
                                return "Number:"+value;
                            }
                        },
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(10),
                        Types.STRING
                );
        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "datagenerator")
                .print();
        env.execute();


    }

    private static void kafkatestproducerextracted() {
        String topicName = "test001";
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //循环100次
        for (int i = 0; i < 100; i++) {
            String key = "key-" + i;
            String value = "WIFIProbe|1678256409406|1201|VUZJXzYyMDg=|86:99:C7:B4:86:E9|WPA2-PSK||FF:FF:FF:FF:FF:FF|6|00|08|-50|2.3|0|160|128|||||abcd1243|WIFIProbe-2023-03-08" + i;
            //每次创建一个消息生产者对象
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
            producer.send(record);
        }
        producer.close();
    }
}