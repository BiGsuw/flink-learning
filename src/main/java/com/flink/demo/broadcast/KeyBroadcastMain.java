package com.flink.demo.broadcast;

import com.alibaba.fastjson.JSON;
import com.flink.demo.broadcast.source.MysqlSource;
import com.flink.demo.broadcast.transform.CoKeyedBroadcastProcessFunction;
import com.flink.demo.utils.RunTimeUtils;
import com.flink.demo.pojo.MediaEntity;
import com.flink.demo.utils.SourceUtils;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Properties;

/**
 * key stream broadcast state demo
 * @author EMing Zhou
 * @version 1.0
 * @date 2022/1/14 21:31
 */
public class KeyBroadcastMain {

    //  key Stream
    public static final MapStateDescriptor<Integer, Tuple2<String, Integer>> configKeyBroadcastDescriptor
            = new MapStateDescriptor<>("key-mysql-config-table", Types.INT, Types.TUPLE(Types.STRING, Types.INT));


    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = RunTimeUtils.getRunTimeEnvironment();
        //flink 1.12之前使用的是如下方式，1.13 时已废弃
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //flink 1.13 默认使用的是eventTime，并且自动生成watermark，如果想显式的使用processTime可以把关闭watermark（设置为0）
        env.getConfig().setAutoWatermarkInterval(0);


        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        kafkaProperties.setProperty("auto.offset.reset", "earliest");
        kafkaProperties.setProperty("group.id", "flink-kafka-test");

        //kafka 流
        DataStream<String> kafkaStream = SourceUtils.kafkaSourceUtils(env, kafkaProperties, "test_online");
        SingleOutputStreamOperator<MediaEntity> mediaSource = kafkaStream
                .map(v -> JSON.parseObject(v, MediaEntity.class)).setParallelism(1)
                .name("keyed-kafka-convert-media-map");


        //mysql 配置流
        DataStreamSource<Map<Integer, Tuple2<String, Integer>>> mysqlStream = env.addSource(
                new MysqlSource("127.0.0.1", 3306, "test", "root", "Mysql123456", 60))
                .setParallelism(1);


        //生成broadcast stream ，此处可以添加多个不同类型的Descriptor
        BroadcastStream<Map<Integer, Tuple2<String, Integer>>> broadcastStream =
                mysqlStream.broadcast(configKeyBroadcastDescriptor);


        SingleOutputStreamOperator<MediaEntity> resultStream = mediaSource
                .keyBy(v -> v.mediaId)
                .connect(broadcastStream)
                .process(new CoKeyedBroadcastProcessFunction()).name("kafka-co-mysql-key-broadcast");

        resultStream.print();


        // execute program
        env.execute("Flink Key Broadcast State Demo");
    }
}
