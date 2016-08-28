package com.lianjia.profiling.common.hbase.roller;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.common.hbase.table.TableManager;
import com.lianjia.profiling.util.DateUtil;

import java.io.IOException;
import java.util.*;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

@SuppressWarnings("Duplicates")
public class OnlineUserTableRoller {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineUserTableRoller.class.getName(), new HashMap<String, String>());

    private static final int MAX_RETRY = 100000;
    private Timer timer = new Timer(OnlineUserTableRoller.class.getSimpleName(), true);

    private String tablePrefix;
    private String tableIdxPrefix;

    private volatile String tableName;
    private volatile String tableIdxName;

    private String date;

    public OnlineUserTableRoller(String date, final String tablePrefix, Map<String, String> conf) throws IOException {
        if (!DateUtil.isValidDate(date))  throw new IOException("invalid date format, date: " + date);
        this.date = date;
        this.tablePrefix = tablePrefix;
        this.tableIdxPrefix = this.tablePrefix + "_idx";
    }

    public OnlineUserTableRoller(final String tablePrefix, Map<String, String> conf) throws IOException {
        this.tablePrefix = tablePrefix;
        this.tableIdxPrefix = this.tablePrefix + "_idx";
    }

    private String getSuffix() {
        return date != null ? DateUtil.getMonth(date) : DateUtil.getMonth();
    }

    private class RollingTask extends TimerTask {
        @Override
        public void run() {
            String newTableName = tablePrefix + "_" + getSuffix();
            String newTableIdxName = tableIdxPrefix + "_" + getSuffix();
            LOG.info("rolling table: %s, table idx: %s...", newTableName, newTableIdxName);

            // sleep until new table name != current table name
            while (newTableName.equals(tableName) || newTableIdxName.equals(tableIdxName)) {
                LOG.warn(String.format("new table name: %s or idx table name: %s equals current table name, sleeping...",
                                       newTableName, newTableIdxName));
                try {
                    Thread.sleep(1000);
                    newTableName = tablePrefix + "_" + getSuffix();
                    newTableIdxName = tableIdxPrefix + "_" + getSuffix();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // create tables
            try {
                while (!TableManager.createIfNotExists(newTableName) || !TableManager.createIfNotExists(newTableIdxName)) {
                    LOG.error("create table: %s failed or create table: %s failed, sleeping then retry...",
                              newTableName, newTableName);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            // switch name of current tables
            OnlineUserTableRoller.this.tableName = newTableName;
            OnlineUserTableRoller.this.tableIdxName = newTableIdxName;
            LOG.info("switched to new table: %table and new idx table: %s.", newTableName, newTableIdxName);

            // schedule once
            Date nextMonth = DateUtil.alignedByMonth(DateUtil.getNextMonth());
            LOG.info("[in task] try to rolling, schedule once, next wake up time: %s...", nextMonth);
            timer = new Timer(OnlineUserTableRoller.class.getSimpleName(), true);
            timer.schedule(new RollingTask(), nextMonth);
            LOG.info("[in task] scheduled table rolling, schedule once, next wake up time: %s...", nextMonth);
        }
    };

    private class ListenTask extends TimerTask {
        @Override
        public void run() {
            String newTableName = tablePrefix + "_" + getSuffix();
            String newIdxTableName = tableIdxPrefix + "_" + getSuffix();

            // sleep until new table exists.
            LOG.info("listening table: %s, table idx: %s...", newTableName, newIdxTableName);
            try {
                while (!TableManager.exists(newTableName) || !TableManager.exists(newIdxTableName)) {
                    LOG.info("table: %s or idx table: %s is not exist, sleeping...", newTableName, newIdxTableName);
                    try {
                        Thread.sleep(1000);
                        newTableName = tablePrefix + "_" + getSuffix();
                        newIdxTableName = tableIdxPrefix + "_" + getSuffix();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            // switch
            OnlineUserTableRoller.this.tableName = newTableName;
            OnlineUserTableRoller.this.tableIdxName = newIdxTableName;
            LOG.info("switched to new table: %s, new idx table: %s.", newTableName, newIdxTableName);

            // schedule once
            Date nextMonth = DateUtil.alignedByMonth(DateUtil.getNextMonth());
            timer = new Timer(OnlineUserTableRoller.class.getSimpleName(), true);
            timer.schedule(new ListenTask(), nextMonth);
            LOG.info("listening, schedule once, next wake up time: %s...", nextMonth);
        }
    };

    public void roll() throws IOException {
        String newTableName = this.tablePrefix + "_" + getSuffix();
        String newIdxTableName = this.tableIdxPrefix + "_" + getSuffix();

        // create table if table does not exist
        int tries = 1;
        while ((!TableManager.createIfNotExists(newTableName) || !TableManager.createIfNotExists(newIdxTableName)) &&
                tries <= MAX_RETRY) {
            LOG.error("create table %s or idx table: %s failed, sleeping and retry...", newTableName, newIdxTableName);
            try {
                Thread.sleep(1000);
                tries++;
                newTableName = this.tablePrefix + "_" + getSuffix();
                newIdxTableName = this.tableIdxPrefix + "_" + getSuffix();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (tries > MAX_RETRY) {
            LOG.error("create table %s or idx table: %s failed", newTableName, newIdxTableName);
            throw new IOException(String.format("create table %s or idx table: %s failed", newTableName, newIdxTableName));
        }

        // schedule once
        Date nextMonth = DateUtil.alignedByMonth(DateUtil.getNextMonth());
        timer.schedule(new RollingTask(), nextMonth);
        LOG.info("scheduled table rolling, schedule once, next wake up time: %s...", nextMonth);
    }

    public void listen() throws IOException {
        String newTableName = OnlineUserTableRoller.this.tablePrefix + "_" + getSuffix();
        String newIdxTableName = OnlineUserTableRoller.this.tableIdxPrefix + "_" + getSuffix();

        LOG.info("listening table: %s, table idx: %s...", newTableName, newIdxTableName);

        /**
         * check table existence有时会socket connect超时异常. 打个patch(20160621), 只在每个月第一天的第一个小时内判断. 其他情况默认table存在.
         */
        long timeDiffMs = System.currentTimeMillis() - DateUtil.alignedByMonth().getTime();
        while (timeDiffMs < 60 * 60 * 1000 && !TableManager.exists(newTableName) || !TableManager.exists(newIdxTableName)) {
            LOG.info("table: %s or idx table: %s are not exist, sleeping...", newTableName, newIdxTableName);
            try {
                Thread.sleep(1000);
                newTableName = OnlineUserTableRoller.this.tablePrefix + "_" + getSuffix();
                newIdxTableName = OnlineUserTableRoller.this.tableIdxPrefix + "_" + getSuffix();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        this.tableName = newTableName;
        this.tableIdxName = newIdxTableName;

        // schedule once
        Date nextMonth = DateUtil.alignedByMonth(DateUtil.getNextMonth());
        timer.schedule(new ListenTask(), nextMonth);
        LOG.info("listening, schedule once, next wake up time: %s...", nextMonth);
    }

    public String getTable() {
        return tableName;
    }

    public String getTableIdx() {
        return tableIdxName;
    }
}
