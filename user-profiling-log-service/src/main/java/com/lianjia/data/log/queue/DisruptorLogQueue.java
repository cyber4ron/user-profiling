package com.lianjia.data.log.queue;

import com.lianjia.data.log.LogQueue;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class DisruptorLogQueue<E> implements LogQueue<E> {

    public void put(E ele) throws InterruptedException {
    }

    public E take() throws InterruptedException {
        throw new InterruptedException();
    }
}
