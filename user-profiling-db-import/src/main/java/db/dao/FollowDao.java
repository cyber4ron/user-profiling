package db.dao;

import com.alibaba.fastjson.JSON;
import com.lianjia.profiling.common.BlockingBackoffRetryProxy;
import db.domain.FollowInfo;
import db.util.MysqlJdbcUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.update.UpdateRequest;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by huangfangsheng on 16/6/12.
 */

public class FollowDao {
    private static final Log LOG = LogFactory.getLog(FollowDao.class);

    static class BatchFollowHandler implements ResultSetHandler<Integer> {
        private BlockingBackoffRetryProxy proxy;

        public BatchFollowHandler() {
            this.proxy = new BlockingBackoffRetryProxy(new scala.collection.immutable.HashMap<String, String>());
        }

        @Override
        public Integer handle(ResultSet rs) throws SQLException {
            int count = 0;
            while (rs.next()) {
                try {
                    FollowInfo fl = new FollowInfo(Integer.parseInt(rs.getBigDecimal(1).toString()),
                                                   Long.parseLong(rs.getBigDecimal(2).toString()),
                                                   rs.getInt(3),
                                                   rs.getString(4),
                                                   rs.getInt(5),
                                                   rs.getString(6),
                                                   rs.getString(7),
                                                   rs.getInt(8),
                                                   rs.getTimestamp(9).toString(),
                                                   rs.getString(10),
                                                   rs.getString(11));

                    proxy.send(new UpdateRequest(IDX, TYPE, String.valueOf(fl.getFavorite_id()))
                                       .docAsUpsert(true)
                                       .doc(JSON.toJSONString(fl.toHashMap())));

                    // System.out.println(JSON.toJSONString(fl.toHashMap()));
                    count++;

                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                if (count % 1000 == 0) {
                    LOG.info(" ==> " + count + " lines.");
                }
            }

            proxy.flush();

            LOG.info(" ==> " + count + " lines.");

            return count;
        }
    }

    public static final String IDX = "db_follow";
    public static final String TYPE = "fl";

    public static final String FIELDS = "fav.favorite_id, fav.user_id, fav.notification_status, fav.favorite_type, fav.source_type, fav.favorite_condition, fav.condition_detail, fav.bit_status, fav.ctime, fav.mtime, house.hdic_city_id";
    private QueryRunner qr;

    public FollowDao() throws UnknownHostException {
        qr = new QueryRunner(MysqlJdbcUtil.getDataSource()) {
            @Override
            protected PreparedStatement prepareStatement(Connection conn, String sql) throws SQLException {
                conn.setAutoCommit(false);
                PreparedStatement stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                                                               ResultSet.CONCUR_READ_ONLY);
                stmt.setFetchSize(Integer.MIN_VALUE);

                return stmt;
            }
        };
    }

    // 根据id返回 FollowInfo 对象
    public FollowInfo getFollowInfoById(int id) throws SQLException {
        String sql = "select " + FIELDS + " from lianjia.lianjia_user_favorite fav " +
                "join lianjia.house_sell_new house on CONVERT (fav.favorite_condition USING ascii) = house.house_code " +
                "where fav.favorite_id = ?";
        System.out.println("sql:" + sql + ", id: " + id);
        return qr.query(sql, new BeanHandler<>(FollowInfo.class), id);
    }

    // 根据mtime返回 FollowInfo 对象
    public List<FollowInfo> getFollowInfoByMtime(String startTime, String endTime) throws SQLException {
        String sql = "select " + FIELDS + " from lianjia.lianjia_user_favorite fav " +
                "join lianjia.house_sell_new house on CONVERT (fav.favorite_condition USING ascii) = house.house_code " +
                "where fav.mtime >= ? and fav.mtime <= ?";
        System.out.println("sql:" + sql + ", start: " + startTime + ", end: " + endTime);
        return qr.query(sql, new BeanListHandler<>(FollowInfo.class), startTime, endTime);
    }

    /**
     * 慢!
     */
    public int loadFullTable() throws SQLException {
        String sql = "select " + FIELDS + " from lianjia.lianjia_user_favorite fav " +
                "join lianjia.house_sell_new house on CONVERT (fav.favorite_condition USING ascii) = house.house_code";
        return qr.query(sql, new BatchFollowHandler());
    }

    public int loadFullTable(String startTime, String endTime) throws SQLException {
        String sql = "select " + FIELDS + " from lianjia.lianjia_user_favorite fav " +
                "join lianjia.house_sell_new house on CONVERT (fav.favorite_condition USING ascii) = house.house_code " +
                "where fav.mtime >= ? and fav.mtime <= ?";
        System.out.println("sql:" + sql + ", start: " + startTime + ", end: " + endTime);
        return qr.query(sql, new BatchFollowHandler(), startTime, endTime);
    }

    public int loadFullTableTest(String startTime, String endTime) throws SQLException {
        String sql = "select " + FIELDS + " from lianjia.lianjia_user_favorite fav " +
                "join lianjia.house_sell_new house on CONVERT (fav.favorite_condition USING ascii) = house.house_code " +
                "where fav.mtime >= ? and fav.mtime <= ? and fav.favorite_condition = '101091768171' ";
        return qr.query(sql, new BatchFollowHandler(), startTime, endTime);
    }
}

