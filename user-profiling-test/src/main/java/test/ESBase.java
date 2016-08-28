package test;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */

public abstract class ESBase {
    @Option(name = "--cluster-name")
    private String clusterName = "my-application";

    @Option(name = "--host")
    private String host = "172.30.17.2";

    @Option(name = "--port")
    private int port = 9300;

    protected Client client;

    public ESBase parseArgs(String[] args) throws CmdLineException {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);
        return this;
    }

    public ESBase createClient() throws UnknownHostException {
        Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();

        client = TransportClient.builder().settings(settings).build()
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                client.close();
            }
        });

        return this;
    }

    public abstract void run() throws Exception;
}
