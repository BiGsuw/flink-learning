package com.flink.demo.broadcast;

import com.alibaba.fastjson.JSON;
import com.flink.demo.broadcast.source.MysqlSource;
import com.flink.demo.broadcast.transform.CoBroadcastProcessFunction;
import com.flink.demo.pojo.MediaEntity;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * broadcast state demo start class
 * @author EMing Zhou
 * @version 1.0
 * @date 2022/1/12 17:25
 */
public class BroadcastMain {

    // non key Stream
//    public static final MapStateDescriptor<Void, Map<String,Tuple2<String,Integer>>> configBroadcastDescriptor
//            = new MapStateDescriptor<>("mysql-config-table", Types.VOID,
//            Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));


    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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


        //kafka 流
        kafkaProperties.setProperty("group.id","flink-kafka-test");
        FlinkKafkaConsumerBase<String> kafkaSource = new FlinkKafkaConsumer011<>("test_online"
                        ,new SimpleStringSchema(),
                        kafkaProperties);
        DataStream<String> kafkaStream = env
                .addSource(kafkaSource)
                .setParallelism(1)
                .name("kafka_source");

        SingleOutputStreamOperator<MediaEntity> mediaSource = kafkaStream
                .map(v -> JSON.parseObject(v, MediaEntity.class)).setParallelism(1)
                .name("kafka-convert-media-map");

        //mysql 配置流
        DataStreamSource<Map<Integer, Tuple2<String, Integer>>> mysqlStream = env.addSource(
                new MysqlSource("127.0.0.1", 3306, "test", "root", "Mysql123456", 60))
                .setParallelism(1);

        MapStateDescriptor<Void, Map<String,Tuple2<String,Integer>>> configBroadcastDescriptor
                = new MapStateDescriptor<>("mysql-config-table", Types.VOID,
                Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));

        //生成broadcast stream ，此处可以添加多个不同类型的Descriptor
        BroadcastStream<Map<Integer, Tuple2<String, Integer>>> broadcastStream =
                mysqlStream.broadcast(configBroadcastDescriptor);


        SingleOutputStreamOperator<MediaEntity> resultStream = mediaSource
                .connect(broadcastStream)
                .process(new CoBroadcastProcessFunction()).name("kafka-co-mysql-broadcast");

        resultStream.print();


        // execute program
        env.execute("Flink Broadcast State Demo");
    }
}
