package com.lianjia.profiling.stream.parser;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.stream.builder.OnlineUserEventBuilder;
import org.apache.hadoop.hbase.client.Row;

import java.util.HashMap;
import java.util.List;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class FlumeOnlineUserMessageParser extends OnlineUserMessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(FlumeOnlineUserMessageParser.class.getName(), new HashMap<String, String>());

    public static void parse(String line,
                             List<OnlineUserEventBuilder.Doc> userDocs,
                             List<OnlineUserEventBuilder.EventDoc> eventDocs,
                             List<Row> eventsHbase,
                             List<Row> eventsIndicesHbase,
                             String indexName) {
        OnlineUserMessageParser.parse(line, userDocs, eventDocs, eventsHbase, eventsIndicesHbase, indexName);
    }
}
