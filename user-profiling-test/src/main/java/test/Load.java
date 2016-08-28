package test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import test.models.User;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

class BuildJson {
    public static void parseJackson() throws IOException {
        ObjectMapper mapper = new ObjectMapper(); // can reuse, share globally
        String json = "{\n"
            + "  \"name\" : { \"first\" : \"Joe\", \"last\" : \"Sixpack\" },\n"
            + "  \"gender\" : \"MALE\",\n"
            + "  \"verified\" : false,\n"
            + "  \"userImage\" : \"Rm9vYmFyIQ==\"\n"
            + "}";
        User user = mapper.readValue(json, User.class);

        ////
        // generate json
        byte[] json2 = mapper.writeValueAsBytes(user);
    }

    public static String buildMap() {
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("user", "kimchy");
        json.put("postDate", new Date());
        json.put("message", "trying out Elasticsearch");

        return json.toString();
    }

    public static String buildES() throws IOException {
        XContentBuilder builder = jsonBuilder()
            .startObject()
            .field("user", "kimchy")
            .field("postDate", new Date())
            .field("message", "trying out Elasticsearch")
            .endObject();
        return builder.string();
    }
}

class ESTools {
    public static void index() throws IOException {
        String jsEs = BuildJson.buildES();
        String jsMap = BuildJson.buildMap();

        // on startup
        Node node = nodeBuilder().node();
        Client client = node.client();
        IndexResponse response = client.prepareIndex("twitter", "tweet", "1")
            .setSource(jsEs)
            .get();

        // Index name
        String _index = response.getIndex();
        // Type name
        String _type = response.getType();
        // Document ID (generated or not)
        String _id = response.getId();
        // Version (if it's the first time you index this document, you will get: 1)
        long _version = response.getVersion();
        // isCreated() is true if the document is a new one, false if it has been updated
        boolean created = response.isCreated();

        // on shutdown
        node.close();
    }
}

public class Load {
    public static void main(String[] args) throws IOException {
        System.out.println("starting..");
        ESTools.index();
        System.out.println("returning...");
    }
}
