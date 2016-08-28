package com.lianjia.profiling.tool;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author fenglei@lianjia.com on 2016-08
 */

public class ParsePopularHouse {

    public static void main(String[] args) throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get("user-profiling-tool/src/main/resources/popular_house"));
        JSONArray array  = JSON.parseArray(new String(bytes));

        StringBuilder res = new StringBuilder();
        for(Object house: array) {
            String houseId = ((JSONObject)house).get("target").toString();
            res.append(houseId).append("\n");
        }

        Path path = Paths.get("user-profiling-tool/src/main/resources/popular_house2");
        Files.write(path, res.toString().getBytes());
    }
}
