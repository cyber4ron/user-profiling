package com.lianjia.profiling.web.controller;

import com.lianjia.profiling.web.controller.search.HouseSearchController;
import org.junit.Test;

import java.net.UnknownHostException;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class HouseKVControllerTest {

    HouseSearchController ctrl;

    public HouseKVControllerTest () throws UnknownHostException {
        ctrl = new HouseSearchController();
    }

    @Test
    public void testGetOfflineHouseByQuery() throws Exception {
        // System.out.println(ctrl.searchOfflineHouses("resblock_id:1111027373926 AND floors_num:16"));
        // System.out.println(ctrl.searchOfflineHouses("resblock_name:东土城路13号院 AND building_name:2号楼"));
    }
}
