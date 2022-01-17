package com.flink.demo.utils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author EMing Zhou
 * @version 1.0
 * @date 2022/1/14 22:21
 */
public class RunTimeUtils {


    public static StreamExecutionEnvironment getRunTimeEnvironment(){
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
