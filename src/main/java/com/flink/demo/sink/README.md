#ClickHouse

 - ClickHouse Link:https://clickhouse.com/docs/en/quick-start

## Prepare Data
 - port:18888
 ```
    nc -l 18888
 ```
```
1,lisi,20
5,wangwu,30
6,zz,100
``` 

 - ClickHouse SQL
 
```` 
CREATE TABLE IF NOT EXISTS default.t_user(id UInt16,
    name String,
    age UInt16 ) ENGINE = TinyLog();
````
 
````
    select * from default.t_user;
```` 

 
## ClickHouseMain
    # ClickHouseTest
 