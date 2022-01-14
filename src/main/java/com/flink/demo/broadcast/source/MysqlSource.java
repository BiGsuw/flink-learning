package com.flink.demo.broadcast.source;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author EMing Zhou
 * @version 1.0
 * @date 2022/1/12 17:41
 */
public class MysqlSource  extends RichSourceFunction<Map<Integer, Tuple2<String, Integer>>> {

    private static final Logger logger = LoggerFactory.getLogger(MysqlSource.class);

    private final String host; //ip
    private final Integer port; //端口
    private final String dbName; //数据库名称
    private final String user; //账号
    private final String passwd; //密码
    private final Integer secondInterval; //读取的间隔单位(秒)

    private volatile boolean isRunning = true;

    private Connection connection;

    private PreparedStatement preparedStatement;


    public MysqlSource(String host, Integer port, String db, String user, String passwd, Integer secondInterval) {
        this.host = host;
        this.port = port;
        this.dbName = db;
        this.user = user;
        this.passwd = passwd;
        this.secondInterval = secondInterval;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connection= DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/"+dbName+"?useUnicode=true&characterEncoding=UTF-8", user, passwd);
        String sql="select media_id,media_name,media_type from media_config";
        preparedStatement=connection.prepareStatement(sql);
    }


    @Override
    public void close() throws Exception {
        super.close();

        if(connection!=null){
            connection.close();
        }

        if(preparedStatement !=null){
            preparedStatement.close();
        }
    }


    @Override
    public void run(SourceContext<Map<Integer, Tuple2<String, Integer>>> ctx) {
        try {
            while (isRunning){
                Map<Integer, Tuple2<String, Integer>> output = new HashMap<>();
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()){
                    int mediaId = resultSet.getInt("media_id");
                    String mediaName = resultSet.getString("media_name");
                    int mediaType = resultSet.getInt("media_type");

                    output.put(mediaId,new Tuple2<>(mediaName,mediaType));
                }
                ctx.collect(output);
                //每隔多少秒执行一次查询
                Thread.sleep(1000 * secondInterval);
            }
        }catch (Exception e){
            logger.error("数据源装载异常",e);
        }


    }


    @Override
    public void cancel() {
        isRunning = false;
    }
}

