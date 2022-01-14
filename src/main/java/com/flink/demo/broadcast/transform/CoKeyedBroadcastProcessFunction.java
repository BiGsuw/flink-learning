package com.flink.demo.broadcast.transform;

import com.flink.demo.pojo.MediaEntity;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author EMing Zhou
 * @version 1.0
 * @date 2022/1/14 21:46
 */
public class CoKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<Integer, MediaEntity, Map<Integer, Tuple2<String, Integer>>, MediaEntity> {

    private static final Logger logger = LoggerFactory.getLogger(CoKeyedBroadcastProcessFunction.class);

    /**定义MapStateDescriptor*/
    public static final MapStateDescriptor<Integer, Tuple2<String,Integer>> configDescriptor =
            new MapStateDescriptor<>("key-mysql-config-table", Types.INT, Types.TUPLE(Types.STRING, Types.INT));

    @Override
    public void processElement(MediaEntity value, ReadOnlyContext ctx, Collector<MediaEntity> out) throws Exception {

        ReadOnlyBroadcastState<Integer, Tuple2<String, Integer>> broadcastState = ctx.getBroadcastState(configDescriptor);
        Tuple2<String, Integer> tuple2Tmp = broadcastState.get(value.mediaId);

        try {
            if(tuple2Tmp != null){
                System.out.println("processElement：" + value.toString());
                System.out.println("key:"+value.mediaId +",value:" + tuple2Tmp.toString());
                value.mediaName = tuple2Tmp.f0;
                value.mediaType = tuple2Tmp.f1;
            }
            out.collect(value);
        } catch (Exception e) {
            logger.error("run process error {}",e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void processBroadcastElement(Map<Integer, Tuple2<String, Integer>> value, Context ctx, Collector<MediaEntity> out) throws Exception {
        System.out.println("processBroadcastElement："+value.toString());
        for(Integer key :value.keySet()){
            ctx.getBroadcastState(configDescriptor).put(key,value.get(key));
        }
    }

}
