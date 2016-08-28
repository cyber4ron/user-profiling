package com.lianjia.profiling;

import com.lianjia.profiling.common.elasticsearch.index.IndexManager;
import com.lianjia.profiling.config.Constants;
import org.apache.commons.io.IOUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-05
 */

public class ESIndexHelper {
    private static Map<String, String> mappings = new HashMap<>();

    static {
        try {
            InputStream in = ESIndexHelper.class.getResourceAsStream("/online_user.json");
            byte[] bytes = IOUtils.toByteArray(in);
            mappings.put(Constants.ONLINE_USER_IDX, new String(bytes));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private IndexManager idxMgr;

    @Option(name = "--clear-and-create")
    private String indexToCreate;

    public ESIndexHelper() throws UnknownHostException {
        idxMgr = new IndexManager(Collections.<String, String>emptyMap());
    }

    public ESIndexHelper parseArgs(String[] args) throws CmdLineException {
        CmdLineParser parser = new CmdLineParser(this);
        parser.parseArgument(args);
        return this;
    }

    public void run() {
        if (indexToCreate != null) {
            String indexKey = indexToCreate.split("_")[0];
            System.out.println(String.format("creating %s, use %s.", indexToCreate, indexKey));
            idxMgr.clearAndCreate(indexToCreate, mappings.get(indexKey));
        }
    }

    public static void main(String[] args) throws CmdLineException, UnknownHostException {
        new ESIndexHelper().parseArgs(args).run();
    }
}
