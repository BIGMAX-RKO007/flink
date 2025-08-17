package org.example.CDC;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceBuilder;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class OracleToRedisSyncJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000); // 启用 Checkpoint

        // ✅ 使用新的 Incremental Snapshot 模式
        env.fromSource(
                        new OracleSourceBuilder<String>()
                                .hostname("localhost")
                                .port(1521)
                                .databaseList("XE")
                                .schemaList("SECURITIES_USER")
                                .tableList("SECURITIES_USER.USER_POSITIONS")
                                .username("securities_user")
                                .password("Sec123456")
                                // ✅ 使用推荐的反序列化器
                                .deserializer(new JsonDebeziumDeserializationSchema())
                                .startupOptions(StartupOptions.initial())
                                .serverTimeZone("Asia/Shanghai")
                                .splitSize(8096)
                                .fetchSize(1024)
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "Oracle-CDC-Parallel-Source"
                )
                .setParallelism(4) // ✅ 并行读取全量数据
                .map(json -> {
                    // 在这里处理 JSON 并写入 Redis
                    System.out.println("Received: " + json);
                    return json;
                })
//                .map(json -> {
//                    try (Jedis jedis = jedisPool.getResource()) {
//                        JsonNode node = mapper.readTree(json);
//                        String op = node.get("op").asText();
//                        String key = node.get("after") != null ? node.get("after").get("id").asText() : node.get("before").get("id").asText();
//                        Pipeline pipeline = jedis.pipelined();
//                        if ("c".equals(op) || "u".equals(op)) {
//                            pipeline.set("key:" + key, json);
//                        } else if ("d".equals(op)) {
//                            pipeline.del("key:" + key);
//                        }
//                        pipeline.sync();
//                        return json;
//                    } catch (Exception e) {
//                        System.err.println("Failed to process: " + json + ", error: " + e.getMessage());
//                        throw e;
//                    }
//                })
                .setParallelism(1)
                .print();

        env.execute("Flink CDC: Oracle -> Redis Sync");
    }

}
