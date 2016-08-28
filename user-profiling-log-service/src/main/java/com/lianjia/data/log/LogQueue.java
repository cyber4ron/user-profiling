package com.lianjia.data.log;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public interface LogQueue<E> {
    public void put(E ele) throws InterruptedException;

    public E take() throws InterruptedException;
}
