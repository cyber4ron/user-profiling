package com.lianjia.data.profiling.log.client;

import com.lianjia.data.log.service.LogProto;
import com.lianjia.data.log.service.LogTransportGrpc;
import com.lianjia.profiling.util.ExUtil;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;


/**
 * todo: support various message delivering semantic?
 * todo: async send api
 * todo; add host info
 * todo: formatter, bytecode?
 *
 * @author fenglei@lianjia.com on 2016-03.
 */
public class BlockingLogger extends BaseLogger {
    private ManagedChannel channel;
    private LogTransportGrpc.LogTransportBlockingStub blockingStub;

    public BlockingLogger(String className, String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true)
                .build();
        blockingStub = LogTransportGrpc.newBlockingStub(channel);
        int idx = className.indexOf(classNamePrefixMask);
        this.className = idx == -1 ? className : className.substring(classNamePrefixMask.length(), className.length());
        info("logger initiated.");
    }

    public void close() throws Exception {
        info("logger closing...");
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private void write(String message) {
        LogProto.Log log = LogProto.Log.newBuilder().setBody(message).build();
        LogProto.Response resp;
        try {
            resp = blockingStub.transport(log);
            if (resp.getStatus() != 0) {
                System.err.println("failed logging to remote, message: " + resp.getMessage());
            }
        } catch (StatusRuntimeException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void debug(String message) {
        super.debug(format(Level.DEBUG, message));
        write(format(Level.DEBUG, message));
    }

    @Override
    public void debug(String format, Object ... args) {
        debug(String.format(format, args));
    }

    @Override
    public void info(String message) {
        super.info(format(Level.INFO, message));
        write(format(Level.INFO, message));
    }

    @Override
    public void info(String format, Object ... args) {
        info(String.format(format, args));
    }


    @Override
    public void warn(String message) {
        super.warn(format(Level.WARN, message));
        write(format(Level.WARN, message));
    }

    @Override
    public void warn(String format, Object ... args) {
        warn(String.format(format, args));
    }

    @Override
    public void warn(String message, Throwable th) {
        warn(String.format("%s ex: %s", message, ExUtil.getStackTrace(th)));
    }

    @Override
    public void error(String message) {
        super.error(format(Level.ERROR, message));
        write(format(Level.ERROR, message));
    }

    @Override
    public void error(String format, Object ... args) {
        error(String.format(format, args));
    }
}
