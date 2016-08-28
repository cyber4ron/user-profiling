package com.lianjia.data.profiling.log;

import com.lianjia.data.profiling.log.client.BlockingLogger;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */
public class LoggingClientTest {
    public static void main(String[] args) throws Exception {

        BlockingLogger client = new BlockingLogger(LoggingClientTest.class.getName(), "localhost", 50051);

        // test ser
        FileOutputStream fileOut =
                new FileOutputStream("/tmp/employee.ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(client);
        out.close();
        fileOut.close();

        try {
            String user = "test log";
            if (args.length > 0) {
                user = args[0];
            }
            client.info(user);
        } finally {
            client.close();
        }
    }
}
