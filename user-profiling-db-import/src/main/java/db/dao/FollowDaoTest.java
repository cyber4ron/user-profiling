package db.dao;

import org.junit.Test;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class FollowDaoTest {

    @Test
    public void testLoadFullTableTest() throws Exception {
        final FollowDao dao = new FollowDao();
        dao.loadFullTableTest("2016-08-01 12:00:00", "2016-08-03 13:00:00");
    }
}
