package com.es.entity;

import lombok.Data;

import java.util.Date;

/**
 * author: 阿杰
 */
@Data
public class FileObj {
    /**
     * 用于存储文件id
     */
    String id;
    /**
     * 文件名
     */
    String name;
    /**
     * 文件的type，pdf，word，or txt
     */
    String type;
    /**
     * 数据插入时间
     */
    String createTime;
    /**
     * 当前数据所属人员
     */
    String createBy;

    /**
     * 当前数据所属人员的年龄
     */
    int age;

    /**
     * 当前数据所属人员的资产
     */
    int money;

    /**
     * 文件转化成base64编码后所有的内容。
     */
    String content;
}
