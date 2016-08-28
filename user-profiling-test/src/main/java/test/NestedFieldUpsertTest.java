package test;

import org.elasticsearch.action.update.UpdateRequest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-05
 */
public class NestedFieldUpsertTest extends ESBase {
    public static void main(String[] args) throws Exception {
        new NestedFieldUpsertTest().parseArgs(args).createClient().run();
    }

    @Override
    public void run() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("nested_upsert");
        updateRequest.type("nested_upsert");
        updateRequest.id("0009");

        Map<String, Object> doc = new HashMap<>();
        doc.put("id", "0009");

        Map<String, Object> nestedDoc1 = new HashMap<>();
        nestedDoc1.put("nested_id", "0001");
        nestedDoc1.put("prop1", "prop0001");
        Map<String, Object> nestedDoc2 = new HashMap<>();
        nestedDoc2.put("nested_id", "0002");

        Map<String, Object> grandNestedDoc1 = new HashMap<>(); // grandNestedDoc1
        grandNestedDoc1.put("grand_nested_id", "0007");
        grandNestedDoc1.put("grand_prop1", "prop0003");
//        Map<String, Object> grandNestedDoc2 = new HashMap<>(); // grandNestedDoc2
//        grandNestedDoc2.put("grand_nested_id", "0002");
//        grandNestedDoc2.put("grand_prop1", "prop0002");
//        Map<String, Object> grandNestedDoc3 = new HashMap<>(); // grandNestedDoc3
//        grandNestedDoc3.put("grand_nested_id", "0002");
//        grandNestedDoc3.put("grand_prop1", "prop0002");
        nestedDoc2.put("grand_nested", Arrays.asList(grandNestedDoc1/*, grandNestedDoc2, grandNestedDoc3*/));

        // 上面注释掉之后会抹去原有数据! 所以只能用脚本append

        doc.put("nested", Arrays.asList(nestedDoc1, nestedDoc2));

        updateRequest.doc(doc);
        updateRequest.docAsUpsert(true);
        client.update(updateRequest).get();
    }
}
