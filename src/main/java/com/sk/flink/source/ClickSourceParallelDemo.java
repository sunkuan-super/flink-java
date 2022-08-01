package com.sk.flink.source;

import com.sk.flink.bean.Event;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author: create by sunkuan
 * @Description:
 * @date: 2022/7/21 - 22:59
 */
public class ClickSourceParallelDemo implements ParallelSourceFunction<Event> {

    // 设置一个全局的标志位
    private boolean running = true;

    private Random random = new Random();
    String[] users = {"Mary", "Bob", "John", "James"};
    String[] urls = {"./home", "./cart", "./fav", "prod?id=100", "prod?id=10"};

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        while (running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            long timeInMillis = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user, url, timeInMillis));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
