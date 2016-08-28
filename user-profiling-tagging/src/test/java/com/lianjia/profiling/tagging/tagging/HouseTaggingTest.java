package com.lianjia.profiling.tagging.tagging;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lianjia.profiling.tagging.house.HouseTagging;
import com.lianjia.profiling.tagging.features.HouseProperties;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class HouseTaggingTest {

    @Test
    public void testCompute() throws Exception {
        String json = new String(Files.readAllBytes(Paths.get("src/main/resources/house.json")));

        JSONObject house = (JSONObject) JSON.parseObject(json).get("data");

        HouseProperties prop = HouseTagging.compute(house);

        System.out.println(prop.toJson());
        System.out.println();
    }
}
