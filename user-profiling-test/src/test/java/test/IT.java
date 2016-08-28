package test;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class IT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {

        System.out.println(System.getProperty("tests.security.manager"));

        return Settings.settingsBuilder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(Node.HTTP_ENABLED, true)
            .build();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void test() throws Exception {
         Client client = client();

        String jsEs = BuildJson.buildES();
        String jsMap = BuildJson.buildMap();

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

        for(String x: client.settings().names()) {
            System.out.println(x + ", " + client.settings().get(x));
        }

        // get
        GetResponse response2 = client.prepareGet("twitter", "tweet", "1").get();


        Thread.sleep(1000 * 1000);
    }
}

