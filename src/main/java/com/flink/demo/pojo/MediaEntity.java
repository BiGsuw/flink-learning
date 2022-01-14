package com.flink.demo.pojo;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * 媒体实体类
 * @author EMing Zhou
 * @version 1.0
 * @date 2022/1/14 11:26
 */
@Data
@EqualsAndHashCode
public class MediaEntity implements Serializable{

    private static final long serialVersionUID = 4703735781214213944L;

    public int mediaId; //ID
    public long cost; //消耗
    public String mediaName; //媒体名称
    public int mediaType; //媒体类型 0：内渠 ， 1：外发

}
