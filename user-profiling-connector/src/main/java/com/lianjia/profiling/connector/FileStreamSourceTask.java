package com.lianjia.profiling.connector;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileSystem;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/**
 * 参考https://github.com/apache/kafka/blob/0.9.0/connect/file/src/main/java/org/apache/kafka/connect/file/FileStreamSourceTask.java
 *
 * 目录同一时刻只能tail一个文件, 已经切换的文件不会再读. 可以忽略有特定tag的文件, 如错误日志
 */
@SuppressWarnings("unchecked")
public class FileStreamSourceTask extends SourceTask {
    class NewFileWatcher {
        public void watchDirectoryPath(Path path) {
            try {
                Boolean isFolder = (Boolean) Files.getAttribute(path, "basic:isDirectory", NOFOLLOW_LINKS);
                if (!isFolder) {
                    throw new IllegalArgumentException("Path: " + path + " is not a folder");
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

            System.out.println("watching path: " + path);

            FileSystem fs = path.getFileSystem();
            BufferedReader newReader;
            try (WatchService service = fs.newWatchService()) {

                path.register(service, ENTRY_CREATE);

                WatchKey key;
                while (true) {
                    key = service.take();
                    WatchEvent.Kind<?> kind;
                    for (WatchEvent<?> watchEvent : key.pollEvents()) {
                        kind = watchEvent.kind();
                        if (OVERFLOW == kind) {
                            System.out.println("OVERFLOW");
                        } else {
                            if (ENTRY_CREATE == kind) {
                                try {
                                    String absPath = path + "/" + ((WatchEvent<Path>) watchEvent).context().toString();
                                    System.out.println("detect new file: " + absPath);
                                    if (!absPath.endsWith(".log")) {
                                        System.out.println(absPath + " not end with .log, abort and continue watching...");
                                        break;
                                    }

                                    FileReader fr = new FileReader(absPath);
                                    newReader = new BufferedReader(fr);
                                    System.out.println("new reader created: " + newReader.toString());

                                    BufferedReader oldReader = reader;
                                    FileStreamSourceTask.this.updateReader(newReader);
                                    oldReader.close();
                                    System.out.println("old reader closed: " + oldReader.toString());

                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }

                    if (!key.reset()) {
                        break;
                    }
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static final String FILENAME_FIELD = "filename";
    public static final String POSITION_FIELD = "position";

    private BufferedReader reader;
    private long streamOffset;
    private String topic;
    private NewFileWatcher dirWatcher;

    private static String hostname = "Unknown";

    static {
        try {
            InetAddress addr;
            addr = InetAddress.getLocalHost();
            hostname = addr.getHostName();
        } catch (UnknownHostException ex) {
            System.out.println("Hostname can not be resolved");
        }
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * 准备input stream, 并skip已经度过的行
     */
    @Override
    public void start(final Map<String, String> props) {
        String filename = props.get(FileStreamSourceConnector.FILE_CONFIG);
        try {
            Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(FileStreamSourceConnector.FILE_CONFIG,
                                                                                                       filename));
            InputStream stream = new FileInputStream(filename);
            if (offset != null) {
                Object lastRecordedOffset = offset.get(POSITION_FIELD);
                if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long))
                    throw new ConnectException("Offset position is the incorrect type");
                if (lastRecordedOffset != null) {
                    System.out.println("Found previous offset, trying to skip to file offset " + lastRecordedOffset);
                    long skipLeft = (Long) lastRecordedOffset;
                    while (skipLeft > 0) {
                        try {
                            long skipped = stream.skip(skipLeft);
                            skipLeft -= skipped;
                        } catch (IOException e) {
                            System.out.println("Error while trying to seek to previous offset in file: ");
                            e.printStackTrace();
                            throw new ConnectException(e);
                        }
                    }
                    System.out.println("Skipped to offset " + lastRecordedOffset);
                }
                streamOffset = (lastRecordedOffset != null) ? (Long) lastRecordedOffset : 0L;
            } else {
                streamOffset = 0L;
            }
            reader = new BufferedReader(new InputStreamReader(stream));
            System.out.println(String.format("Opened %s for reading", filename));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        topic = props.get(FileStreamSourceConnector.TOPIC_CONFIG);

        Thread watcher = new Thread() {
            @Override
            public void run() {
                dirWatcher = new NewFileWatcher();
                dirWatcher.watchDirectoryPath(Paths.get(props.get(FileStreamSourceConnector.DIR_CONFIG)));
            }
        };

        watcher.start();
    }

    public synchronized void updateReader(BufferedReader reader) {
        this.reader = reader;
        this.streamOffset = 0;
    }

    public boolean filter(String line) {
        return line.startsWith("NOTICE:") && line.contains("request_id=") && !line.contains("'uuid': 'heart_beat'");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            ArrayList<SourceRecord> records = new ArrayList<>();
            while (records.isEmpty()) {
                synchronized (this) {
                    String line = reader.readLine(); // 会block, 就不是poll了. 另外readLine() is not interruptable
                    if (line != null) {
                        if (filter(line)) {
                            // System.out.println(line); //todo: partition设置?
                            Map<String, ?> sourcePartition = offsetKey(reader.toString());
                            Map<String, ?> sourceOffset = offsetValue(streamOffset);
                            records.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA,
                                                         hostname + ":" + line));
                        }
                        streamOffset += 1;
                    } else {
                        Thread.sleep(100);
                    }
                }
            }
            return records;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public synchronized void stop() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }
}
