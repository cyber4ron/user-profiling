package com.lianjia.profiling.tool;

import com.lianjia.profiling.util.DateUtil;
import com.lianjia.profiling.util.SMSUtil;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class ProcessMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessMonitor.class.getName());
    private static final String MAIL = "mail";
    private static final String MESSAGE = "message";

    private String processTag;
    private long interval;
    private Timer timer = new Timer(ProcessMonitor.class.getName(), false);

    private Map<String, String> commands = new HashMap<>();

    public ProcessMonitor(String tag, String keyPrefix, long interval) throws IOException {
        this.processTag = tag;
        this.interval = interval;
        LOG.info("tag: " + this.processTag);

        String line;
        try (InputStream stream = ProcessMonitor.class.getResourceAsStream("/monitor.properties")) {
            BufferedReader input = new BufferedReader(new InputStreamReader(stream));
            while ((line = input.readLine()) != null) {
                if (line.trim().isEmpty() || line.trim().startsWith("#") || !line.startsWith(keyPrefix)) continue;

                String[] parts = line.split("=");
                if(parts.length == 1) parts = Arrays.asList(parts[0], "").toArray(new String[2]); // key =

                LOG.info(String.format("key = %s, value = %s.",
                                       parts[0].trim().substring(keyPrefix.length() + 1),
                                       parts[1].trim()));
                commands.put(parts[0].trim().substring(keyPrefix.length() + 1), parts[1].trim());
            }
        }

        TimerTask aliveTask = new TimerTask() {
            @SuppressWarnings("ConstantConditions")
            @Override
            public void run() {
                try {
                    SMSUtil.sendMail(String.format("%s monitor is alive", processTag),
                                     String.format("%s monitor is alive, %s", processTag, DateUtil.toDateTime(System.currentTimeMillis()).substring(0, 15)));
                } catch (Exception e) {
                    LOG.warn("", e);
                }
            }
        };

        timer.scheduleAtFixedRate(aliveTask, new DateTime(DateTimeZone.forOffsetHours(8)).plusDays(1).withTimeAtStartOfDay().toDate(),
                                  24 * 60 * 60 * 1000);
    }

    /**
     * kill后台运行的父进程, 子进程不受影响
     */
    @SuppressWarnings("ConstantConditions")
    public void restart() {
        try {
            if(commands.get("level").equalsIgnoreCase(MAIL)) {
                LOG.info("sending mail...");
                SMSUtil.sendMail(String.format("%s restarting...", processTag),
                                 String.format("%s restarting, %s", processTag,
                                               DateUtil.toDateTime(System.currentTimeMillis()).substring(0, 15)));

            } else if (commands.get("level").equalsIgnoreCase(MESSAGE)) {
                String time = DateUtil.toDateTime(System.currentTimeMillis()).substring(0, 15);
                LOG.info("sending mail...");
                SMSUtil.sendMail(String.format("%s restarting...", processTag),
                                 String.format("%s restarting, %s", processTag, time));

                LOG.info("sending message...");
                SMSUtil.sendMessage(String.format("%s restarting, %s", processTag, time));
            }

            LOG.info("executing cmd: " + commands.get("restart"));
            ProcessBuilder builder = new ProcessBuilder(commands.get("restart"));

            String time = DateUtil.toDateTime(System.currentTimeMillis()).substring(0, 15);

            // redirect
            builder.redirectOutput(new File(String.format(commands.get("log"), time)));
            builder.redirectError(new File(String.format(commands.get("log"), time)));

            Process p = builder.start();

            BufferedReader stdin = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader stderr = new BufferedReader(new InputStreamReader(p.getErrorStream()));

            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = stderr.readLine()) != null) {
                sb.append(line);
            }
            if (!sb.toString().isEmpty()) LOG.warn("stderr: " + sb.toString());

            sb = new StringBuilder();
            while ((line = stdin.readLine()) != null) {
                sb.append(line);
            }
            LOG.info("stdin: " + sb.toString());

        } catch (Exception e) {
            LOG.warn("", e);
        }
    }

    public void monitor() {
        TimerTask monitorTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    Process p = Runtime.getRuntime().exec(commands.get("monitor"));

                    BufferedReader stdin = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    BufferedReader stderr = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                    String line;
                    StringBuilder sb = new StringBuilder();
                    while ((line = stderr.readLine()) != null) {
                        sb.append(line);
                    }
                    if (!sb.toString().isEmpty()) LOG.warn("stderr: " + sb.toString());

                    boolean found = false;
                    while ((line = stdin.readLine()) != null) {
                        if (!line.contains(ProcessMonitor.class.getName()) && line.contains(processTag)) {
                            LOG.info("line contains tag: " + line);
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        LOG.warn("process missing, restarting...");
                        restart();
                    }

                } catch (Exception e) {
                    LOG.warn("", e);
                }
            }
        };

        timer.scheduleAtFixedRate(monitorTask, 0, this.interval);
        LOG.info("task scheduled.");
    }

    public static void main(String[] args) throws IOException {
        ProcessMonitor monitor = new ProcessMonitor(args[0], args[1], Long.parseLong(args[2]));
        monitor.monitor();
    }
}
