package com.lianjia.profiling.web.domain;

import com.lianjia.profiling.web.common.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class Request {
    public static class OlapRequest {
        public String sql = "";
        public String token = "";

        public OlapRequest(){}

        public OlapRequest(String sql, String token) {
            this.sql = sql;
            this.token = token;
        }
    }

    public static class SearchRequest {
        public String query = "";
        public String sql = "";
        public int pageNo = 0;
        public int pageSize = Constants.SIZE_LIMIT;
        public String token = "";

        public SearchRequest(){}

        public SearchRequest(String query, String sql, int pageNo, int pageSize, String token) {
            this.query = query;
            this.sql = sql;
            this.pageNo = pageNo;
            this.pageSize = pageSize;
            this.token = token;
        }
    }

    public static class UserOnlineSearchRequest {
        public String query = "";
        public String sql = "";
        public String start = "";
        public String end = "";
        public int pageNo = 0;
        public int pageSize = Constants.SIZE_LIMIT;
        public String token = "";

        public UserOnlineSearchRequest(){}

        public UserOnlineSearchRequest(String query, String sql, String start, String end, int pageNo, int pageSize, String token) {
            this.query = query;
            this.sql = sql;
            this.start = start;
            this.end = end;
            this.pageNo = pageNo;
            this.pageSize = pageSize;
            this.token = token;
        }
    }

    public static class BatchKVRequest {
        public List<String> ids = new ArrayList<>();
        public String token = "";

        public BatchKVRequest(){}

        public BatchKVRequest(List<String> ids, String token) {
            this.ids = ids;
            this.token = token;
        }
    }

    public static class BatchKVLongRequest {
        public List<Long> ids = new ArrayList<>();
        public String token = "";

        public BatchKVLongRequest(){}

        public BatchKVLongRequest(List<Long> ids, String token) {
            this.ids = ids;
            this.token = token;
        }
    }
}
