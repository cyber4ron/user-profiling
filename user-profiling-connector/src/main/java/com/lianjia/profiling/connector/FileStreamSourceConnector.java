package com.lianjia.profiling.connector;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class FileStreamSourceConnector extends SourceConnector {
    public static final String FILE_CONFIG = "filename";
    public static final String TOPIC_CONFIG = "topic";
    public static final String DIR_CONFIG = "dir";
    private String filename; // initial file to tail
    private String topic; // kafka topic to send message
    private String dir; // dir to watch

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        filename = props.get(FILE_CONFIG);
        topic = props.get(TOPIC_CONFIG);
        dir = props.get(DIR_CONFIG);
    }

    @Override
    public void stop() {
        // Nothing to do since no background monitoring is required.
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileStreamSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();

        // Only one input stream makes sense.
        Map<String, String> config = new HashMap<>();
        config.put(FILE_CONFIG, filename);
        config.put(TOPIC_CONFIG, topic);
        config.put(DIR_CONFIG, dir);
        configs.add(config);

        return configs;
    }

}
