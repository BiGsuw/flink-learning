package com.flink.demo.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

/**
 * 数据源工具类
 * @author EMing Zhou
 * @version 1.0
 * @date 2022/1/17 20:06
 */
public class SourceUtils {


    /**
     * kafka source
     * @param env 执行环境
     * @param kafkaProperties kafka配置
     * @param topic topic
     * @return DataStream
     */
    public static DataStream<String> kafkaSourceUtils(StreamExecutionEnvironment env,
                                                       Properties kafkaProperties,
                                                       String topic) {
        //kafka 流
        FlinkKafkaConsumerBase<String> kafkaSource = new FlinkKafkaConsumer011<>(topic
                ,new SimpleStringSchema(),
                kafkaProperties);

        return env.addSource(kafkaSource).setParallelism(1).name("kafka_source");
    }
}
