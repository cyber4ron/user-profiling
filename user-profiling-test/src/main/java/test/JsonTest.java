package test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lianjia.profiling.model.DelegationEvent;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.commons.lang3.StringEscapeUtils.unescapeJava;

public class JsonTest {
    public void kafkaMessageTest() {

        ObjectMapper mapper = new ObjectMapper();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get("kafkaMessage"), Charset.forName("UTF-8"))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                System.out.println("====> " + line);
                System.out.println("====> " + line.replace("\\\"", "\""));
                System.out.println("====> " + line.replace("\\\"", "\"").replace("\\\\", "\\"));
                System.out.println("====> " + unescapeJava(line)); // 这里结果和上一行一样. 确保所有的escape都能被去掉一层.
               // mapper.readValue()
            }

        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }
    }

    public void jsonTest() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();

        DelegationEvent del = new DelegationEvent();
        // del.setNeedBalcony(true);
        String json = mapper.writeValueAsString(del);
        System.out.println(json);
    }

    public static void main(String[] args) {
        new JsonTest().kafkaMessageTest();
    }
}
