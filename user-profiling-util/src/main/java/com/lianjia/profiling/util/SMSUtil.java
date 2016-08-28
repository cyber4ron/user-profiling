package com.lianjia.profiling.util;

import com.alibaba.fastjson.JSON;
import org.apache.http.Consts;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class SMSUtil {
    private static final Logger LOG = Logger.getLogger(SMSUtil.class);

    private static final String USER_AGENT = "User-Agent";
    private static final String VERSION = "version";
    private static final String METHOD = "method";
    private static final String GROUP = "group";
    private static final String AUTH = "auth";
    private static final String PHONE = "phone";
    private static final String TEMPLATE = "template";
    private static final String PARAMS = "params";

    private static final String URL = "http://sms.lianjia.com/lianjia/sms/send";
    private static final String MAIL_TO = "fenglei@lianjia.com";
    private static final String MESSAGE_TO = "15201551057";

    private static final HttpClient CLIENT = HttpClientBuilder.create().build();
    private static final RequestConfig REQUEST_CONFIG = RequestConfig.custom()
            .setConnectionRequestTimeout(5000)
            .setConnectTimeout(5000)
            .setSocketTimeout(5000)
            .build();

    public static void sendMail(Object subject, Object body, String mailTo) throws Exception {
        Map<String, Object> postParams = new HashMap<>();
        postParams.put(VERSION, "1.0");
        postParams.put(METHOD, "mail.sent");
        postParams.put(GROUP, "bigdata");
        postParams.put(AUTH, "yuoizSsKggkjOc8vbMwS0OqYHvwTGGbB");

        Map<String, Object> params = new HashMap<>();
        params.put("to", Arrays.asList(mailTo.split(",")));
        params.put("subject", subject);
        params.put("body", body);

        postParams.put(PARAMS, params);

        post(postParams);
    }

    public static void sendMail(Object subject, Object body) throws Exception {
        sendMail(subject, body, MAIL_TO);
    }

    public static void sendMessage(String message) throws Exception {
        Map<String, Object> postParams = new HashMap<>();
        postParams.put(VERSION, "1.0");
        postParams.put(METHOD, "sms.sent");
        postParams.put(GROUP, "bigdata");
        postParams.put(AUTH, "yuoizSsKggkjOc8vbMwS0OqYHvwTGGbB");
        postParams.put(TEMPLATE, "bigdatatemplate");
        postParams.put(PHONE, MESSAGE_TO);

        Map<String, Object> params = new HashMap<>();
        params.put("content", message);

        postParams.put(PARAMS, params);

        post(postParams);
    }

    private static void post(Map<String, Object> postParams) throws IOException {
        HttpPost post = new HttpPost(URL);

        post.setConfig(REQUEST_CONFIG);
        post.addHeader(USER_AGENT, "Mozilla/5.0");
        post.setEntity(new StringEntity(JSON.toJSONString(postParams), Charset.forName(Consts.UTF_8.name())));

        try {
            LOG.info("Sending 'POST' request to URL : " + URL);
            LOG.info("Post parameters : " + post.getEntity());
            HttpResponse response = CLIENT.execute(post);

            LOG.info("Response Code : " + response.getStatusLine().getStatusCode());

            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

            StringBuilder result = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }

            LOG.info(result.toString());

        } finally {
            post.releaseConnection();
        }
    }
}
