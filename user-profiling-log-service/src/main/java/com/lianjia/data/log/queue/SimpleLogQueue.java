package com.lianjia.data.log.queue;

import com.lianjia.data.log.LogQueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class SimpleLogQueue<E> implements LogQueue<E> {
    private BlockingQueue<E> queue = new LinkedBlockingQueue<>();

    public void put(E ele) throws InterruptedException {
        queue.put(ele);
    }

    public E take() throws InterruptedException {
        return queue.take();
    }
}
