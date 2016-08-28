package com.lianjia.data.log;

import com.lianjia.data.log.appender.LocalAppender;
import com.lianjia.data.log.queue.SimpleLogQueue;
import com.lianjia.data.log.service.LogProto;
import com.lianjia.data.log.service.LogTransportGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * todo: multiple appenders. one appender one queue? 总共一个queue也行(fanout), 不过要有一个cleaner专门dequeue(dequeue大家都访问过的)
 */
public class LogService {
    private static final Logger logger = Logger.getLogger(LogService.class.getName());

    private class LogTransportImpl implements LogTransportGrpc.LogTransport {
        private LogQueue<String> queue;

        public LogTransportImpl(LogQueue<String> queue) {
            this.queue = queue;
        }

        @Override
        public void transport(LogProto.Log req, StreamObserver<LogProto.Response> responseObserver) {
            try {
                System.err.println("log received, log: " + req.getBody()); // for debug
                queue.put(req.getBody());

                LogProto.Response resp = LogProto.Response.newBuilder().setStatus(0).setMessage("message enqueued.").build();
                responseObserver.onNext(resp);
                responseObserver.onCompleted();

            } catch (InterruptedException e) {
                e.printStackTrace();

                LogProto.Response resp = LogProto.Response.newBuilder().setStatus(1).setMessage("exception: " + e.getMessage()).build();
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
            }
        }
    }

    /* The port on which the server should run, todo add args */
    private int port = 50051;
    private Server server;
    public String logPath = "log/log";

    private LogQueue<String> queue = new SimpleLogQueue<>();

    private void start() throws IOException {
        Appender appender = new LocalAppender(queue, logPath);
        appender.start();

        server = ServerBuilder.forPort(port)
                .addService(LogTransportGrpc.bindService(new LogTransportImpl(queue)))
                .build()
                .start();
        logger.info("Server started, listening on " + port);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                LogService.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final LogService server = new LogService();
        server.start();
        server.blockUntilShutdown();
    }
}
