package com.lianjia.profiling.batch.hdfs;

import com.lianjia.profiling.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class HDFSClient {
    private static final String HDFS_URL = Properties.get("hdfs.url");
    private static FileSystem fs;

    static {
        Configuration configuration = new Configuration();
        try {
            fs = FileSystem.get(new URI(HDFS_URL), configuration);
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static boolean exists(String path) {
        Path file = new Path(HDFS_URL + path);
        try {
            return fs.exists(file);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static long getModTime(String p) {
        Path path = new Path(HDFS_URL + p);
        try {
            return fs.getFileStatus(path).getModificationTime();
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }
}
