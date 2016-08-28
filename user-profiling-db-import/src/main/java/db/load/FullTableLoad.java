package db.load;

import db.dao.FollowDao;

import java.net.UnknownHostException;
import java.sql.SQLException;

/**
 * Created by huangfangsheng on 16/6/12.
 */

public class FullTableLoad {

    public static void main(String[] args) throws SQLException, UnknownHostException {
        load(args);
    }

    public static void load(String[] args) throws SQLException, UnknownHostException {
        FollowDao dao = new FollowDao();
        dao.loadFullTable(args[0], args[1]);
    }
}

