package com.flink.demo.asyncIO.transform;

import com.alibaba.druid.pool.DruidDataSource;
import com.flink.demo.pojo.MediaEntity;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.*;


/**
 * 异步读取mysql function
 * @author EMing Zhou
 * @version 1.0
 * @date 2022/1/18 11:08
 */
public class AsyncFromMysqlFunction extends RichAsyncFunction<MediaEntity, MediaEntity> {

    private static final Logger logger = LoggerFactory.getLogger(AsyncFromMysqlFunction.class);

    private final int maxConnection; // 最大链接数

    private transient DruidDataSource dataSource = null; //druid 连接池

    private transient ExecutorService executorService = null; //线程 服务

    private transient volatile Cache<Integer, Tuple2<String,Integer>> tbMediaCache; // 本地缓存

    public AsyncFromMysqlFunction(int maxConnection) {
        this.maxConnection = maxConnection;
    }

    @Override
    public void open(Configuration parameters) {

        //1、生成一个多线程池
        executorService = Executors.newFixedThreadPool(maxConnection);

        //2、构造一个数据库链接实例
        createDatabaseInstance();

        //3、初始化 tbMediaCache  1缓存
        tbMediaCache = CacheBuilder.newBuilder()
                .initialCapacity(20)
                .maximumSize(30)
                .expireAfterAccess(1, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public void asyncInvoke(MediaEntity input, ResultFuture<MediaEntity> resultFuture) {
        //1、异步回调方法体
        Future<MediaEntity> future = executorService.submit(() -> {
            int mediaId = input.mediaId;
            Connection connection = dataSource.getConnection();
            PreparedStatement preparedStatement = null;
            ResultSet resultSet = null;

            try {
                if (mediaId > 0) {
                    //1.1、从缓存中读取维表数据
                    Tuple2<String, Integer> tmpTuple2 = tbMediaCache.getIfPresent(mediaId);

                    //1.2、如果缓存中没有则从数据库读取
                    if (tmpTuple2 == null) {
                        //这里也可以一次性读取所有的数据，加载到cache里
                        String sql = "select media_id,media_name,media_type from media_config where media_id = ? limit 1";
                        preparedStatement = connection.prepareStatement(sql);
                        preparedStatement.setInt(1, mediaId);
                        resultSet = preparedStatement.executeQuery();
                        while (resultSet.next()) {
                            String mediaName = resultSet.getString("media_name");
                            int mediaType = resultSet.getInt("media_type");
                            input.mediaName = mediaName;
                            input.mediaType = mediaType;
                            tbMediaCache.put(mediaId, new Tuple2<>(mediaName, mediaType));
                        }
                    } else {
                        input.mediaName = tmpTuple2.f0;
                        input.mediaType = tmpTuple2.f1;
                    }
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            } finally {
                if ( resultSet!= null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
            return input;
        });

        //2、数据完成后返回
        CompletableFuture<MediaEntity>  completableFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return future.get();
            } catch (Exception e) {
                logger.error("异步可能会产生中断");
                e.printStackTrace();
                return null;
            }
        });

        //3、返回结果
        completableFuture.thenAccept((MediaEntity m) -> resultFuture.complete(Collections.singleton(m)));
    }

    @Override
    public void timeout(MediaEntity input, ResultFuture<MediaEntity> resultFuture) {
        logger.info("这里是外面定义的1 m 没有结果返回时，会进入这个方法，可以在这里做超时的逻辑处理，" +
                "MediaEntity:{},ResultFuture:{}", input, resultFuture );
    }

    /**
     * 创建数据库实例
     * 最好的方式是通过配置读取，这里为了简便直接写死了
     */
    private synchronized void  createDatabaseInstance() {
        if(dataSource == null){
            synchronized (AsyncFromMysqlFunction.class){
                if(dataSource == null){
                    dataSource = new DruidDataSource();
                    dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
                    dataSource.setUsername("root");
                    dataSource.setPassword("Mysql123456");
                    dataSource.setUrl("jdbc:mysql://127.0.0.1/test?characterEncoding=utf8");
                    dataSource.setMaxActive(maxConnection);
                }
            }

        }
    }
}
