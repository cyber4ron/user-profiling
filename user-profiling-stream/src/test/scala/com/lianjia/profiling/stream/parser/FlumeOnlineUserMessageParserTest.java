package com.lianjia.profiling.stream.parser;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class FlumeOnlineUserMessageParserTest {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testParse() throws Exception {
        // FlumeOnlineUserMessageParser.parse("INFO:\t2016-04-28 13:00:02\tip[114.247.60.199]\ttoken[939207291]\t[bigdata.mobile]\t_\t{\"timestamp\":1461819601,\"network_type\":2,\"events\":\"[{\\\"timestamp\\\":1461658277,\\\"city_id\\\":\\\"110000\\\",\\\"event_id\\\":5,\\\"duration\\\":46,\\\"current_page\\\":\\\"2nd_hand_house_list\\\",\\\"channel_id\\\":2},{\\\"timestamp\\\":1461658286,\\\"city_id\\\":\\\"110000\\\",\\\"event_id\\\":5,\\\"duration\\\":8,\\\"current_page\\\":\\\"2nd_hand_house_detail\\\",\\\"channel_id\\\":2},{\\\"timestamp\\\":1461658292,\\\"city_id\\\":\\\"110000\\\",\\\"event_id\\\":5,\\\"duration\\\":5,\\\"current_page\\\":\\\"2nd_hand_house_list\\\",\\\"channel_id\\\":2},{\\\"timestamp\\\":1461658296,\\\"city_id\\\":\\\"110000\\\",\\\"event_id\\\":5,\\\"duration\\\":3,\\\"current_page\\\":\\\"search_suggest\\\",\\\"channel_id\\\":2},{\\\"timestamp\\\":1461658297,\\\"city_id\\\":\\\"110000\\\",\\\"filter_items\\\":\\\"{\\\\\\\"city_id\\\\\\\":\\\\\\\"110000\\\\\\\",\\\\\\\"conditionsHistory\\\\\\\":{\\\\\\\"empty\\\\\\\":false,\\\\\\\"queryStr\\\\\\\":\\\\\\\"一品亦庄\\\\\\\",\\\\\\\"title\\\\\\\":\\\\\\\"一品亦庄\\\\\\\"},\\\\\\\"is_suggestion\\\\\\\":0,\\\\\\\"query_str\\\\\\\":\\\\\\\"一品亦庄\\\\\\\"}\\\",\\\"keywords\\\":\\\"一品亦庄\\\",\\\"search_source\\\":1,\\\"event_id\\\":2,\\\"sug_position\\\":-1,\\\"result_numbers\\\":0,\\\"use_sug\\\":0,\\\"channel_id\\\":2}]\",\"device_type\":\"Lenovo Lenovo P780\",\"app_source_type\":\"Android_Tencent\",\"app_version\":\"6.12.1\",\"longitude\":116.453281,\"latitude\":39.978192,\"uuid\":\"860485025001241\",\"os_version\":\"4.2.1\",\"os_type\":\"1\"}", null, null, null, null, null);
    }
}
