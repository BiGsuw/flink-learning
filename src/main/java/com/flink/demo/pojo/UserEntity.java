package com.flink.demo.pojo;

public class UserEntity {
    public int id;
    public String name;
    public int age;

    public UserEntity(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public UserEntity(String v) {
        String[] split = v.split(",");
        this.id = Integer.valueOf(split[0]);
        this.name = split[1];
        this.age = Integer.valueOf(split[2]);
    }

    public UserEntity of(int id, String name, int age) {
        return new UserEntity(id, name, age);
    }
 
    // 构造Value 部分(1,'李四',20)
    public static String convertToCsv(UserEntity user) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");

        // add user.id
        sb.append(user.id);
        sb.append(", ");
 
        // add user.name
        sb.append("'");
        sb.append(String.valueOf(user.name));
        sb.append("', ");
 
        // add user.age
        sb.append(user.age);

        sb.append(" )");
        return sb.toString();
    }
}
