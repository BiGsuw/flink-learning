package com.flink.demo.broadcast.transform;

import com.flink.demo.pojo.MediaEntity;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author EMing Zhou
 * @version 1.0
 * @date 2022/1/14 14:59
 */
public class CoBroadcastProcessFunction extends BroadcastProcessFunction<MediaEntity,
        Map<Integer, Tuple2<String, Integer>>, MediaEntity> {

    private static final Logger logger = LoggerFactory.getLogger(CoBroadcastProcessFunction.class);

    /**定义MapStateDescriptor*/
    public static final MapStateDescriptor<Void, Map<Integer, Tuple2<String,Integer>>> configDescriptor =
            new MapStateDescriptor<>("mysql-config-table", Types.VOID,
                    Types.MAP(Types.INT, Types.TUPLE(Types.STRING, Types.INT)));

    /*
    仔细查看context 是只读的，因为kafka流进来是进入processElement处理，如果这里能对广播流修改就会发生什么情况呢
    1、每次的修改并不能准时的同步给所有的task，因为broadcast 是分发到每一个task的内存里面的，并不能跨task传输
    2、这次修改了，下次mysql刷新数据的时候如果不使用managed state来处理会出现丢失的情况,因为这中间需要把当前的一个备份下来要不然相同
       都是null key 会直接覆盖
     */
    @Override
    public void processElement(MediaEntity value, ReadOnlyContext ctx, Collector<MediaEntity> out) throws Exception {

        ReadOnlyBroadcastState<Void, Map<Integer, Tuple2<String, Integer>>> broadcastState =
                ctx.getBroadcastState(configDescriptor);
        Map<Integer, Tuple2<String, Integer>> configMap = broadcastState.get(null);

        try {
            if(configMap != null){
                System.out.println("processElement：" + value.toString());
                for(Integer key : configMap.keySet()){
                    System.out.println("key: "+ key +",value: " + configMap.get(key));
                }
                Tuple2<String, Integer> tuple2Tmp = configMap.get(value.mediaId);
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
        ctx.getBroadcastState(configDescriptor).put(null,value);

    }
}
