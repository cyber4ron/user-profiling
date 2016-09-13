package com.lianjia.profiling.stream;

import com.lianjia.profiling.common.BlockingBackoffRetryProxy;
import com.lianjia.profiling.common.RequestBuilder;
import com.lianjia.profiling.common.hbase.client.BlockingBatchWriteHelper;
import com.lianjia.profiling.stream.builder.OnlineUserEventBuilder;
import com.lianjia.profiling.stream.parser.OnlineUserMessageParser;
import org.apache.hadoop.hbase.client.Row;
import org.elasticsearch.action.update.UpdateRequest;
import scala.collection.Seq;
import scala.collection.immutable.HashMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * todo: 需要再查下转换是否正确
 *
 * @author fenglei@lianjia.com on 2016-04
 */
public class OnlineUserTest {
    public static void main(String[] args) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get("kafkaMessage"), Charset.forName("UTF-8"));
             BlockingBackoffRetryProxy esProxy = new BlockingBackoffRetryProxy(new HashMap<String, String>());
             BlockingBatchWriteHelper hbaseProxy = new BlockingBatchWriteHelper("tmp:tbl1")) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                List<Row> puts = new ArrayList<>();
                List<OnlineUserEventBuilder.Doc> users = new ArrayList<>();
                List<OnlineUserEventBuilder.EventDoc> events = new ArrayList<>();
                List<Object[]> redisKVs = new ArrayList<>();
                List<Row> eventIdx = new ArrayList<>();

                OnlineUserMessageParser.parse(line, users, events, puts, eventIdx, redisKVs, "online_user"); // todo online_user

                for (Row put : puts) {
                    System.out.println(put.toString());
                    hbaseProxy.send(put);
                }

                RequestBuilder rb = RequestBuilder.newReq();
                for (OnlineUserEventBuilder.Doc user : users) {
                    System.out.println(user.toString());
                    rb.setIdentity(user.idx, user.idxType, user.id).addUpsertReq(user.doc);
                }

                for (OnlineUserEventBuilder.EventDoc evt : events) {
                    System.out.println(evt.toString());
                    rb.setIdentity(evt.idx, evt.idxType, evt.id).addUpsertReq(evt.doc);
                }

                Seq<UpdateRequest> reqs = rb.get();
                for (int i = 0; i < reqs.size(); i++) {
                    esProxy.send(reqs.head()); //
                }

                try {
                    //noinspection ResultOfMethodCallIgnored
                    System.in.read();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
