package com.sk.flink.bean;

import org.apache.hadoop.yarn.util.Times;

import java.sql.Timestamp;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/8/5 - 23:13
 */
public class UrlViewCount {
    public String url;

    public Long count;

    public long windowStart;

    public long windowEnd;

    public UrlViewCount(String url, Long count, long windowStart, long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public UrlViewCount() {
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
