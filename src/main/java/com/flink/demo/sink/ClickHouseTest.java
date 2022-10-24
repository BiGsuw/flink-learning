package com.flink.demo.sink;


import com.flink.demo.pojo.UserEntity;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import ru.ivi.opensource.flinkclickhousesink.ClickHouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
 

/**
 * 写入ClickHouse
 * @author EMing Zhou
 * @version 1.0
 * @date 2022/10/24 17:05
 */
public class ClickHouseTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        Map<String, String> globalParameters = new HashMap<>();
 
        // ClickHouse cluster properties
        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, "http://127.0.0.1:8123/");
        //默认不需要账号密码，如需要可以打开下面的注释
//        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_USER, "xxx");
//        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_PASSWORD, "xxxx");
 
        // sink common
        globalParameters.put(ClickHouseSinkConst.TIMEOUT_SEC, "10");
        globalParameters.put(ClickHouseSinkConst.FAILED_RECORDS_PATH, "/tmp/clickhouse/failed_data");
        globalParameters.put(ClickHouseSinkConst.NUM_WRITERS, "2");
        globalParameters.put(ClickHouseSinkConst.NUM_RETRIES, "2");
        globalParameters.put(ClickHouseSinkConst.QUEUE_MAX_CAPACITY, "2");
        globalParameters.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, "false");
 
        // set global paramaters
        ParameterTool parameters = ParameterTool.fromMap(globalParameters);
        env.getConfig().setGlobalJobParameters(parameters);
 

        // source
        DataStream<String> sourceStream = env.socketTextStream("localhost", 18888);
        sourceStream.print();

        //transform
        SingleOutputStreamOperator<String> userStream = sourceStream
                .map(v -> new UserEntity(v))
                .setParallelism(1)
                .name("convert_user_map")
                .map(v -> UserEntity.convertToCsv(v))
                .setParallelism(1)
                .name("convert_csv_map");


        // create props for sink
        Properties props = new Properties();
        props.put(ClickHouseSinkConst.TARGET_TABLE_NAME, "default.t_user");
        props.put(ClickHouseSinkConst.MAX_BUFFER_SIZE, "10000");
        ClickHouseSink sink = new ClickHouseSink(props);
        userStream.addSink(sink).name("sink_to_clickhouse");
        userStream.print();
 
        env.execute("clickhouse sink test");
    }
}
