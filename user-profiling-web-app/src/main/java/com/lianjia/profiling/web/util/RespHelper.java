package com.lianjia.profiling.web.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author fenglei@lianjia.com on 2016-04
 */

public class RespHelper {
    public static class HouseEvalSuccResponse {
        public HouseEvalSuccResponse(long count) {
            this.data = new HashMap<>();
            data.put("house_value_total_usage", count);
        }

        @JSONField(name = "request_id", ordinal = 0)
        public String request_id = UUID.randomUUID().toString();

        @JSONField(name = "data", ordinal = 1)
        public Map<String, Long> data;

        @JSONField(name = "errmsg", ordinal = 2)
        public String errmsg = "";

        @JSONField(name = "error", ordinal = 3)
        public int error = 0;
    }

    public static class HouseEvalFailResponse {
        @JSONField(name = "request_id", ordinal = 0)
        public int request_id;

        @JSONField(name = "errmsg", ordinal = 1)
        public String errmsg;

        @JSONField(name = "error", ordinal = 2)
        public int error;

        public HouseEvalFailResponse(final int errCode, final String msg) {
            errmsg = msg;
            error = errCode;
        }
    }

    public static class FailResponse {
        @JSONField(name = "error")
        public Object error;

        public FailResponse(int errCode, String msg) {
            error = new Object() {
                @JSONField(name = "code")
                public int errorCode = errCode;

                @JSONField(name = "message")
                public String message = msg;
            };
        }
    }

    public static String getSuccResp(Object data) {
        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "ok");
        resp.put("data", data);
        return JSON.toJSONString(resp);
    }

    public static <T> String getListResp(List<T> data) {
        Map<String, Object> resp = new HashMap<>();
        resp.put("data", data);
        resp.put("num", data.size());

        return JSON.toJSONString(resp);
    }

    public static <T> String getPagedListResp(List<T> data, int page, int size) {
        Map<String, Object> resp = new HashMap<>();
        resp.put("data", data);
        resp.put("num", data.size());
        resp.put("page_no", page);
        resp.put("page_size", size);

        return JSON.toJSONString(resp);
    }

    public static <T> String getTimedListResp(List<T> data, long ms) {
        Map<String, Object> resp = new HashMap<>();
        resp.put("data", data);
        resp.put("num", data.size());
        resp.put("took", ms);

        return JSON.toJSONString(resp);
    }

    public static <T> String getTimedPagedListResp(List<T> data, int page, int size, long ms) {
        Map<String, Object> resp = new HashMap<>();
        resp.put("data", data);
        resp.put("num", data.size());
        resp.put("page_no", page);
        resp.put("page_size", size);
        resp.put("took", ms);

        return JSON.toJSONString(resp);
    }

    public static String getHouseEvalSuccResp(long count) {
        return JSON.toJSONString(new HouseEvalSuccResponse(count));
    }

    public static String getHouseEvalFailedResp(int errCode, String msg) {
        return JSON.toJSONString(new HouseEvalFailResponse(errCode, msg));
    }

    public static String getFailResponse(int errCode, String msg) {
        return JSON.toJSONString(new FailResponse(errCode, msg));
    }

    public static String getSuccResponseForPopularHouse(Object data) {
        Map<String, Object> ret = new HashMap<>();
        ret.put("errorcode", 0);
        ret.put("data", data);

        return JSON.toJSONString(ret);
    }

    public static String getFailResponseForPopularHouse(int errCode, String msg) {
        Map<String, Object> ret = new HashMap<>();
        ret.put("errorcode", errCode);
        ret.put("errormsg", msg);

        return JSON.toJSONString(ret);
    }

    public static String getSuccResponseForHouseOwnerSide(Object data) {
        Map<String, Object> ret = new HashMap<>();
        ret.put("errorcode", 0);
        ret.put("errmsg", "");
        ret.put("data", data);

        return JSON.toJSONString(ret);
    }

    public static String getFailResponseForHouseOwnerSide(int errCode, String msg) {
        Map<String, Object> ret = new HashMap<>();
        ret.put("errorcode", errCode);
        ret.put("errmsg", msg);

        return JSON.toJSONString(ret);
    }


}
