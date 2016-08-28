package com.lianjia.profiling.stream.builder;

import com.lianjia.profiling.stream.parser.HouseEvalMessageParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class HouseEvalEventBuilderTest {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testParse() throws Exception {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get("house_eval_message"), Charset.forName("UTF-8"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                HouseEvalMessageParser.parse(line);
            }
        }
    }
}
