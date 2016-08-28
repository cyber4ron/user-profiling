package com.lianjia.profiling.common.elasticsearch.index;

import com.lianjia.profiling.common.elasticsearch.ClientFactory;
import com.lianjia.profiling.common.hbase.roller.OnlineUserTableRoller;
import com.lianjia.profiling.util.DateUtil;
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
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class IndexRollerTest {

    private static final int ONLINE_KEEP_DAYS = 7;
    private static final DateTimeZone CST = DateTimeZone.forOffsetHours(8);
    private static final DateTimeFormatter DATE_FMT = DateTimeFormat.forPattern("yyyyMMdd");

    // @Test
    public void testGetSuffix() throws Exception {
        IndexRoller x = new IndexRoller("20160501", "online_user", new HashMap<String, String>());
        System.out.println(x.getSuffix());
    }

    @Test
    public void testRolling() throws Exception {
        Date firstTime = new DateTime(DateTimeZone.forOffsetHours(8)).plusDays(7)
                .withDayOfWeek(DateTimeConstants.MONDAY).withTimeAtStartOfDay().toDate();
        long period = TimeUnit.DAYS.toMillis(7);

        System.out.println(String.format("%s is listening, scheduleAtFixedRate, firstTime: %s, period: %d...",
                                         "xx", firstTime.toString(), period));

        System.out.println(firstTime);
        System.out.println(period);
    }

    @Test
    public void testRoll() throws Exception {

        DateTime dt = new DateTime(DateTimeZone.forOffsetHours(8));
        if (!dt.withDayOfWeek(DateTimeConstants.TUESDAY).equals(dt)) System.out.println("sdf");
    }

    @Test
    public void testRoll1() throws Exception {

        long start = new DateTime(CST).minusDays(ONLINE_KEEP_DAYS + 1).withTimeAtStartOfDay().toInstant().getMillis();
        long end = new DateTime(CST).minusDays(ONLINE_KEEP_DAYS).withTimeAtStartOfDay().toInstant().getMillis();

        System.out.println(new DateTime(CST).minusDays(ONLINE_KEEP_DAYS + 1).withTimeAtStartOfDay());
        System.out.println(new DateTime(CST).minusDays(ONLINE_KEEP_DAYS).withTimeAtStartOfDay());

        System.out.println(start);
        System.out.println(end);
    }

    @Test
    public void testRoll2() throws Exception {
        DateTime dt = new DateTime(CST).minusDays(ONLINE_KEEP_DAYS + 1);
        String indexToDeleteData = "xxx" + "_" + DateUtil.alignedByWeek(DATE_FMT.print(dt));

        System.out.println(indexToDeleteData);
    }


    @Test
    public void testDeleteData() throws Exception {
        IndexRoller indexRoller = new IndexRoller("online_user", Collections.<String, String>emptyMap());
        indexRoller.deleteData();
    }

    @Test
    public void deleteByQuery() throws UnknownHostException {
        Client client = ClientFactory.getClient();

        QueryBuilder qb = QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.existsQuery("ucid"));

        // 很慢, 删一周的数据至少要1天
        DeleteByQueryResponse rsp = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                .setIndices("online_user_20160613",
                            "online_user_20160606",
                            "online_user_20160530",
                            "online_user_20160523",
                            "online_user_20160516",
                            "online_user_20160509",
                            "online_user_20160502",
                            "online_user_20160425",
                            "online_user_20160502",
                            "online_user_20160425",
                            "online_user_20160418",
                            "online_user_20160411",
                            "online_user_20160404",
                            "online_user_20160328",
                            "online_user_20160321",
                            "online_user_20160314",
                            "online_user_20160307",
                            "online_user_20160229",
                            "online_user_20160222",
                            "online_user_20160215",
                            "online_user_20160208",
                            "online_user_20160201",
                            "online_user_20160125",
                            "online_user_20160118",
                            "online_user_20160111",
                            "online_user_20160104")
                .setTypes(new String[] {"usr", "dtl", "srh", "fl"})
                .setQuery(qb)
                .get();
    }

    @Test
    public void sched() throws UnknownHostException {
        Timer timer = new Timer(OnlineUserTableRoller.class.getSimpleName(), true);
        TimerTask task = new TimerTask() {
            @Override
            public void run() {}
        };
        timer.schedule(task , 0);
        timer = new Timer(OnlineUserTableRoller.class.getSimpleName(), true);
        timer.schedule(task , 0);
    }

}
