package com.lianjia.profiling.stream.parser;

import com.lianjia.profiling.stream.MessageUtil;
import com.lianjia.profiling.stream.builder.OnlineUserEventBuilder;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class HouseEvalMessageParserTest {

    @Test
    public void testParse() throws Exception {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get("message_house_eval"), Charset.defaultCharset())) {
            String line;
            int cnt = 0;
            while ((line = reader.readLine()) != null) {
                OnlineUserEventBuilder.Doc x = HouseEvalMessageParser.parse(line);
                if(x!= null) {
                    cnt++;
                } else {
                    System.out.println(line);
                }
            }
            System.out.println(cnt);
        }
    }

    @Test
    public void xx() throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get("src/test/resources/flume_message"), Charset.defaultCharset())) {
            String line;
            int cnt = 0;
            while ((line = reader.readLine()) != null) {
                String[] values = MessageUtil.extract(line);
                System.out.println(Arrays.toString(values));
            }
            System.out.println(cnt);
        }
    }
}
