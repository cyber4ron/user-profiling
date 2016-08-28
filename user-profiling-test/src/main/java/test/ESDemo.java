package test;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * @author fenglei@wandoujia.com on 16/2/29.
 */

public class ESDemo {

    static Settings nodeSettings(String pathHome) {
        return Settings.settingsBuilder()
            .put("path.home", pathHome) // ~/Development/elasticsearch-2.2.0
            .build();
    }

    public static void main(String[] args) throws IOException {
        String path = args[0];
        int NF = Integer.valueOf(args[1]);
        String clusterName = args[2];
        String pathHome = args[3];

        System.out.println(path);
        System.out.println(NF);
        System.out.println(clusterName);
        System.out.println(pathHome);

        Node node = nodeBuilder()
            .clusterName(clusterName)
            .settings(nodeSettings(pathHome))
            .client(true) // 注意
            .node();
        Client client = node.client();

        String line = null;
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            while((line = br.readLine()) != null) {
                String[] parts = line.split("\\t");
                if(parts.length != NF) {
                    System.err.println(String.format("%s, %d", line, parts.length));
                    continue;
                }

                XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("pkid", parts[0])
                    .field("custom_code", parts[1])
                    .field("city_code", parts[2])
                    .field("city_name", parts[3])
                    .field("cust_name", parts[4])
                    .field("title", parts[5])
                    .field("title_name", parts[6])
                    .field("phone1_type", parts[7])
                    .field("phone1	", parts[8])
                    .field("phone2_type", parts[9])
                    .field("phone2	", parts[10])
                    .field("phone3_type", parts[11])
                    .field("phone3", parts[12])
                    .field("email", parts[13])
                    .field("biz_type", parts[14])
                    .field("delegation_source", parts[15])
                    .field("delegation_source_child", parts[16])
                    .field("is_login_client", parts[17])
                    .field("login_client_time", parts[18])
                    .field("is_mobile_reg", parts[19])
                    .field("mobile_login_date", parts[20])
                    .field("invalid_time", parts[21])
                    .field("invalid_description", parts[22])
                    .field("status", parts[23])
                    .field("cust_status	string", parts[24])
                    .field("firstshowing_date", parts[25])
                    .field("firstrepeatshowing_date", parts[26])
                    .field("last_showing_time", parts[27])
                    .field("rank	", parts[28])
                    .field("is_chain_custom	", parts[29])
                    .field("is_recommend	", parts[30])
                    .field("etl_date", parts[31])
                    .field("date", Integer.parseInt(parts[32].substring(0, 8)))
                    .endObject();

                String json = builder.string();
                // System.out.println(json);
                // 之后改成bulk
                IndexResponse response = client.prepareIndex("client", "profile", parts[0])
                    .setSource(json)
                    .get();

                if(!response.isCreated()) {
                    System.err.println(String.format("index failed, %s, %s", line, response.toString()));
                }
            }


        }
    }
}
