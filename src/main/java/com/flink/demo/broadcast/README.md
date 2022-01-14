#Broadcast State Pattern

 - Flink Link:https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/datastream/fault-tolerance/broadcast_state/

## Prepare Data
 - Kafka
 
 {"mediaId":472683,"cost":2400}
 {"mediaId":472991,"cost":1200}
 
 - Mysql
 
 CREATE TABLE IF NOT EXISTS media_config(
 	media_id int comment '媒体ID',
 	media_name varchar(128) comment '媒体名称',
 	media_type int comment '媒体类型 0：内渠，1：外发',
 	PRIMARY KEY (media_id)
 );
 
 INSERT INTO  media_config (media_id, media_name, media_type) VALUES (472991, '快手', 0);
 INSERT INTO  media_config (media_id, media_name, media_type) VALUES (472683, '抖音', 1);
 INSERT INTO  media_config (media_id, media_name, media_type) VALUES (473974, '爱奇艺', 0);
 INSERT INTO  media_config (media_id, media_name, media_type) VALUES (454374, '优酷', 0);
 INSERT INTO  media_config (media_id, media_name, media_type) VALUES (476438, '头条', 0);
 
 UPDATE media_config SET media_name='抖音_update' WHERE media_id=472683;
 
 
## BroadcastMain
 - non key Broadcast State 
![result picture](https://github.com/BiGsuw/flink-learning/raw/main/src/main/MarkdownPhotos/broadcast_result.png)


## Key BroadcastMain  
 - key Broadcast State
