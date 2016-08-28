package com.lianjia.profiling.common.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class BufferedMutationHelper {

    private static final int POOL_SIZE = 10;
    private static final int TASK_COUNT = 100;
    private static final TableName TABLE = TableName.valueOf("foo");
    private static final byte[] FAMILY = Bytes.toBytes("f");

    public int run(String[] args) throws InterruptedException, ExecutionException, TimeoutException {

        /** a callback invoked when an asynchronous write fails. */
        final BufferedMutator.ExceptionListener listener;
        BufferedMutatorParams params;

        //
        // step 1: clearAndCreate a single Connection and a BufferedMutator, shared by all worker threads.
        //
        try (final Connection conn = ConnectionFactory.createConnection(new Configuration()); // todo getConf() ???
             final BufferedMutator mutator = conn.getBufferedMutator(new BufferedMutatorParams(TABLE)
                                                                             .listener(new BufferedMutator.ExceptionListener() {
                                                                                 @Override
                                                                                 public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                                                                                     for (int i = 0; i < e.getNumExceptions(); i++) {
                                                                                         //LOG.info("Failed to sent put " + e.getRow(i) + ".");
                                                                                     }
                                                                                 }
                                                                             }))) {

            /** worker pool that operates on BufferedTable instances */
            final ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);
            List<Future<Void>> futures = new ArrayList<>(TASK_COUNT);

            for (int i = 0; i < TASK_COUNT; i++) {
                futures.add(workerPool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        //
                        // step 2: each worker sends edits to the shared BufferedMutator instance. They all use
                        // the same backing buffer, call-back "listener", and RPC executor pool.
                        //
                        Put p = new Put(Bytes.toBytes("someRow"));
                        p.addColumn(FAMILY, Bytes.toBytes("someQualifier"), Bytes.toBytes("some value"));
                        mutator.mutate(p); // add to writeAsyncBuffer(ConcurrentLinkedQueue)
                        // mutator.flush(); // todo 加的
                        // do work... maybe you want to call mutator.flush() after many edits to ensure any of
                        // this worker's edits are sent before exiting the Callable
                        return null;
                    }
                }));
            }

            //
            // step 3: clean up the worker pool, shut down.
            //
            for (Future<Void> f : futures) {
                f.get(5, TimeUnit.MINUTES);
            }
            workerPool.shutdown();
        } catch (IOException e) {
            // exception while creating/destroying Connection or BufferedMutator
            //LOG.info("exception while creating/destroying Connection or BufferedMutator", e);
        } // BufferedMutator.close() ensures all work is flushed. Could be the custom listener is
        // invoked from here.
        return 0;
    }
}
