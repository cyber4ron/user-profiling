package com.lianjia.bigdata.hbase;

import com.lianjia.profiling.hbase.TableManager;
import org.junit.Test;

import java.io.IOException;

/**
 * @author fenglei@lianjia.com on 2016-09
 */

public class TableManagerTest {

    @Test
    public void createTable() throws IOException {
        TableManager.createOnlineUserPreferTable("profiling:online_user_prefer_v3");
    }

}
