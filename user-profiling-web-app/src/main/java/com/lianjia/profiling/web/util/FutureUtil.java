package com.lianjia.profiling.web.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class FutureUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FutureUtil.class);

    private static final ExecutorService executors = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    LOG.info("attempt to shutdown executor");
                    executors.shutdown();
                    executors.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOG.warn("tasks interrupted");
                } finally {
                    if (!executors.isTerminated()) {
                        LOG.info("cancel non-finished tasks");
                    }
                    executors.shutdownNow();
                    LOG.info("shutdown finished");
                }
            }
        });
    }

    public static <T> CompletableFuture<T> getCompletableFuture(Supplier<T> supplier) {
        return CompletableFuture.supplyAsync(supplier::get, FutureUtil.executors);
    }

    public static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futures) {
        CompletableFuture[] cfs = futures.toArray(new CompletableFuture[futures.size()]);

        return CompletableFuture.allOf(cfs).thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList()));
    }
}
