package com.lianjia.profiling.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class RestUtil {
    private static final Logger LOG = Logger.getLogger(RestUtil.class);
    private static final HttpClient CLIENT = HttpClientBuilder.create().build();

    public static String get(String url) {
        try {
            HttpHost target = new HttpHost("172.16.5.24", 8099, "http");

            HttpGet getRequest = new HttpGet(url);
            LOG.info(getRequest.getURI());

            HttpResponse httpResponse = CLIENT.execute(target, getRequest);

            HttpEntity entity = httpResponse.getEntity();
            LOG.info(httpResponse.getStatusLine());

            if (entity != null) {
                return EntityUtils.toString(entity);
            }

            return null;

        } catch (Exception e) {
            LOG.warn(e);
            return null;
        }
    }

    public static String get(String host, int port, String url) {
        try {
            HttpHost target = new HttpHost(host, port, "http");

            HttpGet getRequest = new HttpGet(url);
            LOG.info(getRequest.getURI());

            HttpResponse httpResponse = CLIENT.execute(target, getRequest);

            HttpEntity entity = httpResponse.getEntity();
            LOG.info(httpResponse.getStatusLine());

            if (entity != null) {
                return EntityUtils.toString(entity);
            }

            return null;

        } catch (Exception e) {
            LOG.warn(e);
            return null;
        }
    }
}
