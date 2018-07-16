package com.passionke.student50;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by passionke on 2018/7/12.
 * 紫微无姓，红尘留行，扁舟越沧溟，何须山高龙自灵。
 * 一朝鹏程，快意风云，挥手功名
 */
public class ThreadPool {

    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
            10,
            100,
            1000,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(10),
            Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.AbortPolicy()
            );

    public String pool() {
        threadPoolExecutor.prestartAllCoreThreads();
        threadPoolExecutor.getPoolSize();
        threadPoolExecutor.purge();
        return super.toString();
    }
}
