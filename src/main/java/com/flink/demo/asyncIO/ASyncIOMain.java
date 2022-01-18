package com.flink.demo.asyncIO;

import com.alibaba.fastjson.JSON;
import com.flink.demo.asyncIO.transform.AsyncFromMysqlFunction;
import com.flink.demo.pojo.MediaEntity;
import com.flink.demo.utils.RunTimeUtils;
import com.flink.demo.utils.SourceUtils;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 异步IO 启动类
 * @author EMing Zhou
 * @version 1.0
 * @date 2022/1/17 20:12
 */
public class ASyncIOMain {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = RunTimeUtils.getRunTimeEnvironment();
        //flink 1.12之前使用的是如下方式，1.13 时已废弃
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //flink 1.13 默认使用的是eventTime，并且自动生成watermark，如果想显式的使用processTime可以把关闭watermark（设置为0）
        env.getConfig().setAutoWatermarkInterval(0);

        //设置Checkpoint
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        checkpointConfig.setCheckpointInterval(60 * 1000);
//        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        kafkaProperties.setProperty("auto.offset.reset", "earliest");
        kafkaProperties.setProperty("group.id","flink-kafka-async-test");

        //kafka source
        DataStream<String> kafkaStream = SourceUtils.kafkaSourceUtils(env, kafkaProperties, "test_online");
        SingleOutputStreamOperator<MediaEntity> mediaSource = kafkaStream
                .map(v -> JSON.parseObject(v, MediaEntity.class)).setParallelism(1)
                .name("async-kafka-convert-media-map");
        kafkaStream.print();


        //异步读取mysql维表
        SingleOutputStreamOperator<MediaEntity> resultStream = AsyncDataStream.unorderedWait(mediaSource,
                new AsyncFromMysqlFunction(3),
                6000,
                TimeUnit.MILLISECONDS,
                3)
                .setParallelism(1)
                .name("async_query_from_mysql");
        //打印结果
        resultStream.print();

        // execute program
        env.execute("Flink Async IO Demo");
    }
}
