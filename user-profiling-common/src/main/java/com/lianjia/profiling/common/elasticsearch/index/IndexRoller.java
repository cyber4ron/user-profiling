package com.lianjia.profiling.common.elasticsearch.index;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.common.elasticsearch.ClientFactory;
import com.lianjia.profiling.config.Constants;
import com.lianjia.profiling.util.DateUtil;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class IndexRoller {
    private static final Logger LOG = LoggerFactory.getLogger(IndexRoller.class.getName(), new HashMap<String, String>());

    private static final DateTimeZone CST = DateTimeZone.forOffsetHours(8);
    private static final DateTimeFormatter DATE_FMT = DateTimeFormat.forPattern("yyyyMMdd");

    private static final int ONLINE_DATE_SPLIT_RANGE_DAYS = 7;
    private static final int ONLINE_TOTAL_DATA_RETENTION_DAYS = 7;
    private static final int ONLINE_FILTERED_DATA_RETENTION_DAYS = 180;

    private static final String[] TYPES_TO_DELETE_TOTAL_DATA = new String[] {"usr", "dtl", "srh", "fl"};
    private static final Map<String, String> MAPPINGS = new HashMap<>();

    private Timer timer = new Timer(IndexRoller.class.getSimpleName(), true);

    private IndexManager idxMgr;
    private Client client;

    private String indexPrefix;
    private String indexName;

    private String date;

    static {
        try {
            InputStream in = IndexRoller.class.getResourceAsStream("/online_user.json");
            byte[] bytes = IOUtils.toByteArray(in);
            MAPPINGS.put(Constants.ONLINE_USER_IDX, new String(bytes));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public IndexRoller(String date, final String indexPrefix, Map<String, String> conf) throws IOException {
        if (!DateUtil.isValidDate(date)) throw new IOException("invalid date format, date: " + date);
        this.date = date;
        this.indexPrefix = indexPrefix;

        idxMgr = new IndexManager(conf);
        client = ClientFactory.getClient();
    }

    public IndexRoller(final String indexPrefix, Map<String, String> conf) throws IOException {
        this.indexPrefix = indexPrefix;

        idxMgr = new IndexManager(conf);
        client = ClientFactory.getClient();
    }

    public String getSuffix() {
        return date == null ? DateUtil.alignedByWeek() : DateUtil.alignedByWeek(date);
    }

    public String getSuffixToDel() {
        return DateUtil.alignedByWeek(DateUtil.dateDiff(date == null ? DateUtil.getDate() : date, -ONLINE_FILTERED_DATA_RETENTION_DAYS));
    }

    private void rollIndex() {
        // check if date is monday
        DateTime dt = new DateTime(DateTimeZone.forOffsetHours(8));
        if (!dt.withDayOfWeek(DateTimeConstants.MONDAY).equals(dt)) return;

        String newIndexName = IndexRoller.this.indexPrefix + "_" + getSuffix();
        LOG.info("rolling index: %s...", newIndexName);

        // sleep until new index != old index
        while (newIndexName.equals(IndexRoller.this.indexName)) {
            LOG.warn("newIndexName(%s).equals(DailyRollingIndex.this.indexName(%s)), sleeping...", newIndexName, indexName);
            try {
                Thread.sleep(1000);
                newIndexName = IndexRoller.this.indexPrefix + "_" + getSuffix();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // create index if index does not exist
        if (idxMgr.createIfNotExists(newIndexName, MAPPINGS.get(indexPrefix)) && idxMgr.setReplicas(newIndexName, 0)) {
            LOG.info("created new index: %s.", newIndexName);
            IndexRoller.this.indexName = newIndexName;
        } else {
            LOG.error("failed to create new index: %s.", newIndexName);
        }

        // close old index
        String oldIndexName = IndexRoller.this.indexPrefix + "_" + getSuffixToDel();
        if (idxMgr.close(oldIndexName)) {
            LOG.info("closed old index: %s.", oldIndexName);
        } else {
            LOG.error("failed to close old index: %s.", oldIndexName);
        }
    }

    public void deleteData() {
        DateTime dt = new DateTime(CST).minusDays(ONLINE_TOTAL_DATA_RETENTION_DAYS + 1);
        String indexToDeleteData = indexPrefix + "_" + DateUtil.alignedByWeek(DATE_FMT.print(dt));

        long startTime = System.currentTimeMillis();

        QueryBuilder qb = QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.existsQuery("ucid"));

        LOG.info("query for delete data: %s", qb.toString());

        // 很慢, 删一周的数据至少要1天
        DeleteByQueryResponse rsp = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                .setIndices(indexToDeleteData)
                .setTypes(TYPES_TO_DELETE_TOTAL_DATA)
                .setQuery(qb)
                .get();

        LOG.info("deleted online data that missing ucid field, idx: %s, docs deleted: %d, took %dms.",
                 indexToDeleteData, rsp.getTotalDeleted(), System.currentTimeMillis() - startTime);

        // compact index
        LOG.info("compacting index: %s", indexToDeleteData);
        idxMgr.compactIndex(indexToDeleteData);

        LOG.info("index compacting done: %s", indexToDeleteData);
    }

    public void compactIndex() {
        // double check, 确保index被compact
        DateTime dt = new DateTime(CST).minusDays(ONLINE_TOTAL_DATA_RETENTION_DAYS * 2 + 1);
        String indexToCompact = indexPrefix + "_" + DateUtil.alignedByWeek(DATE_FMT.print(dt));

        LOG.info("compacting index: %s", indexToCompact);
        idxMgr.compactIndex(indexToCompact);

        LOG.info("index compacting done: %s", indexToCompact);
    }

    public void roll() throws IOException {
        if (!MAPPINGS.containsKey(indexPrefix))
            throw new IOException("!MAPPINGS.containsKey(%index)".replace("%index", indexPrefix));
        LOG.info("%s is rolling...", indexPrefix);

        // create index if absent
        String newIndexName = IndexRoller.this.indexPrefix + "_" + getSuffix();
        if (idxMgr.createIfNotExists(newIndexName, MAPPINGS.get(indexPrefix))) {
            this.indexName = newIndexName;
        } else {
            LOG.error("IndexManager.createIfNotExists(%s, MAPPINGS.get(%s)) = false", indexPrefix, indexPrefix);
            throw new IOException(String.format("IndexManager.createIfNotExists(%s, MAPPINGS.get(%s)) = false", indexPrefix, indexPrefix));
        }

        TimerTask rollingTask = new TimerTask() {
            @Override
            public void run() {
                rollIndex();
                compactIndex();
                deleteData();
            }
        };

        Date nextWeek = new DateTime(CST).plusDays(ONLINE_DATE_SPLIT_RANGE_DAYS)
                .withDayOfWeek(DateTimeConstants.MONDAY).withTimeAtStartOfDay().toDate();
        long oneWeekMs = TimeUnit.DAYS.toMillis(ONLINE_DATE_SPLIT_RANGE_DAYS);
        timer.scheduleAtFixedRate(rollingTask, nextWeek, oneWeekMs);

        LOG.info("scheduled index[%s] rolling, scheduleAtFixedRate, firstTime: %s, period: %d...",
                 indexPrefix, nextWeek.toString(), oneWeekMs);
    }

    public void listen() {
        String newIndexName = IndexRoller.this.indexPrefix + "_" + getSuffix();
        poll(newIndexName);
        this.indexName = newIndexName;

        TimerTask listeningTask = new TimerTask() {
            @Override
            public void run() {
                String newIndexName = IndexRoller.this.indexPrefix + "_" + getSuffix();
                LOG.info("waiting index: %s...", newIndexName);

                poll(newIndexName);

                LOG.info("switching new index: %s...", newIndexName);
                IndexRoller.this.indexName = newIndexName;
            }
        };

        Date nextWeek = new DateTime(CST).plusDays(ONLINE_DATE_SPLIT_RANGE_DAYS)
                .withDayOfWeek(DateTimeConstants.MONDAY).withTimeAtStartOfDay().toDate();
        long oneWeekMs = TimeUnit.DAYS.toMillis(ONLINE_DATE_SPLIT_RANGE_DAYS);
        timer.scheduleAtFixedRate(listeningTask, nextWeek, oneWeekMs);

        LOG.info("%s is listening, scheduleAtFixedRate, firstTime: %s, period: %d...",
                 indexPrefix, nextWeek.toString(), oneWeekMs);
    }

    public void poll(String indexName) {
        // sleep until new index exists.
        while (!idxMgr.exist(indexName)) {
            LOG.info("index: %s does't exist, sleeping...", indexName);
            try {
                Thread.sleep(1000);
                indexName = IndexRoller.this.indexPrefix + "_" + getSuffix();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public String getIndex() {
        return indexName;
    }
}
