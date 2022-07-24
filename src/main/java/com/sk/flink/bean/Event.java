package com.sk.flink.bean;

import java.sql.Timestamp;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/21 - 22:47
 */

/**
 * flink支持的POJO类要求有一个空参的构造，属性都是public，方法也都是public
 */
public class Event {

    public String userID;

    public String urls;

    public Long timestamp;

    public Event() {
    }

    public Event(String userID, String urls, long timestamp) {
        this.userID = userID;
        this.urls = urls;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "userID='" + userID + '\'' +
                ", urls='" + urls + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getUrls() {
        return urls;
    }

    public void setUrls(String urls) {
        this.urls = urls;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
