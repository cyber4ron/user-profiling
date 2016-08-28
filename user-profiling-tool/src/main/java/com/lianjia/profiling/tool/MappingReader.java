package com.lianjia.profiling.tool;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class MappingReader {
    public static void main(String[] args) throws IOException {
        // byte[] bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/customer.json"));
        // JSONObject json = JSON.parseObject(new String(bytes));

        // Object obj = ((JSONObject)((JSONObject)((JSONObject)json.get("customer")).get("properties")).get("delegations")).get("properties");
        // Object obj = ((JSONObject)((JSONObject)((JSONObject)json.get("customer")).get("properties")).get("tourings")).get("properties");
        // Object obj = ((JSONObject)((JSONObject)((JSONObject)((JSONObject)((JSONObject)json.get("customer")).get("properties")).get("tourings")).get("properties")).get("houses")).get("properties");
        // Object obj = ((JSONObject)((JSONObject)((JSONObject)json.get("customer")).get("properties")).get("contracts")).get("properties");

        // byte[] bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/customer_contract.json"));
        // JSONObject json = JSON.parseObject(new String(bytes));
        // Object obj = (((JSONObject)json.get("contract")).get("properties"));

        // byte[] bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/house.json"));
        // JSONObject json = JSON.parseObject(new String(bytes));
        // Object obj = (((JSONObject)json.get("house")).get("properties"));

        // byte[] bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/ucid_phone.json"));
        // JSONObject json = JSON.parseObject(new String(bytes));
        // Object obj = json.get("properties");

        // byte[] bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/online_user.json"));
        // JSONObject json = JSON.parseObject(new String(bytes));
        // Object obj = json.get("mappings");

//        byte[] bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/online_user.json"));
//        JSONObject json = JSON.parseObject(new String(bytes));
//        Object obj = ((JSONObject) ((JSONObject) json.get("mappings")).get("usr")).get("properties");
//
//        System.out.println("-- usr");
//        for (Map.Entry<String, Object> e : ((JSONObject) obj).entrySet()) {
//            System.out.println(e.getKey() + "\t" + ((JSONObject) e.getValue()).get("type"));
//        }
//
//        bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/online_user.json"));
//        json = JSON.parseObject(new String(bytes));
//        obj = ((JSONObject) ((JSONObject) json.get("mappings")).get("mob_usr")).get("properties");
//
//        System.out.println("-- mob_usr");
//        for (Map.Entry<String, Object> e : ((JSONObject) obj).entrySet()) {
//            System.out.println(e.getKey() + "\t" + ((JSONObject) e.getValue()).get("type"));
//        }
//
//        bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/online_user.json"));
//        json = JSON.parseObject(new String(bytes));
//        obj = ((JSONObject) ((JSONObject) json.get("mappings")).get("dtl")).get("properties");
//
//        System.out.println("-- dtl");
//        for (Map.Entry<String, Object> e : ((JSONObject) obj).entrySet()) {
//            System.out.println(e.getKey() + "\t" + ((JSONObject) e.getValue()).get("type"));
//        }
//
//        bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/online_user.json"));
//        json = JSON.parseObject(new String(bytes));
//        obj = ((JSONObject) ((JSONObject) json.get("mappings")).get("srh")).get("properties");
//
//        System.out.println("-- srh");
//        for (Map.Entry<String, Object> e : ((JSONObject) obj).entrySet()) {
//            System.out.println(e.getKey() + "\t" + ((JSONObject) e.getValue()).get("type"));
//        }
//
//        bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/online_user.json"));
//        json = JSON.parseObject(new String(bytes));
//        obj = ((JSONObject) ((JSONObject) json.get("mappings")).get("fl")).get("properties");
//
//        System.out.println("-- fl");
//        for (Map.Entry<String, Object> e : ((JSONObject) obj).entrySet()) {
//            System.out.println(e.getKey() + "\t" + ((JSONObject) e.getValue()).get("type"));
//        }
//
//        bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/online_user.json"));
//        json = JSON.parseObject(new String(bytes));
//        obj = ((JSONObject) ((JSONObject) json.get("mappings")).get("mob_dtl")).get("properties");
//
//        System.out.println("-- mob_dtl");
//        for (Map.Entry<String, Object> e : ((JSONObject) obj).entrySet()) {
//            System.out.println(e.getKey() + "\t" + ((JSONObject) e.getValue()).get("type"));
//        }
//
//        bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/online_user.json"));
//        json = JSON.parseObject(new String(bytes));
//        obj = ((JSONObject) ((JSONObject) json.get("mappings")).get("mob_srh")).get("properties");
//
//        System.out.println("-- mob_srh");
//        for (Map.Entry<String, Object> e : ((JSONObject) obj).entrySet()) {
//            System.out.println(e.getKey() + "\t" + ((JSONObject) e.getValue()).get("type"));
//        }
//
//        bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/online_user.json"));
//        json = JSON.parseObject(new String(bytes));
//        obj = ((JSONObject) ((JSONObject) json.get("mappings")).get("mob_fl")).get("properties");
//
//        System.out.println("-- mob_fl");
//        for (Map.Entry<String, Object> e : ((JSONObject) obj).entrySet()) {
//            System.out.println(e.getKey() + "\t" + ((JSONObject) e.getValue()).get("type"));
//        }
//
//        bytes = Files.readAllBytes(Paths.get("user-profiling-model/src/main/mappings/online_user.json"));
//        json = JSON.parseObject(new String(bytes));
//        obj = ((JSONObject) ((JSONObject) json.get("mappings")).get("mob_clk")).get("properties");
//
//        System.out.println("-- mob_clk");
//        for (Map.Entry<String, Object> e : ((JSONObject) obj).entrySet()) {
//            System.out.println(e.getKey() + "\t" + ((JSONObject) e.getValue()).get("type"));
//        }


        byte[] bytes = Files.readAllBytes(Paths.get("user-profiling-tool/src/main/resources/offline_user_demo.json"));
        JSONObject json = JSON.parseObject(new String(bytes));
        Object tourings = (((JSONObject) json.get("data")).get("tourings"));


        for (Object touring : ((JSONArray) tourings)) {
            if (!((JSONObject) touring).containsKey("houses")) continue;
            Object houses = ((JSONObject) touring).get("houses");
            for (Object house : ((JSONArray) houses)) {
                System.out.println(((JSONObject) house).get("house_id") + "\t" + ((JSONObject) house).get("resblock_id"));
            }
        }


    }
}
