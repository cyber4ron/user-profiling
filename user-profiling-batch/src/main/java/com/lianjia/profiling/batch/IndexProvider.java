package com.lianjia.profiling.batch;

import com.lianjia.data.profiling.log.Logger;
import com.lianjia.data.profiling.log.LoggerFactory;
import com.lianjia.profiling.common.elasticsearch.ClientFactory;
import com.lianjia.profiling.common.elasticsearch.index.IndexManager;
import com.lianjia.profiling.util.DateUtil;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-04
 */
public class IndexProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ClientFactory.class.getName(), new HashMap<String, String>());
    private IndexManager idxMgr;

    private static Map<String, String> mappings = new HashMap<>();

    private String custIdxName;
    private String houseIdxName;

    static {
        try {
            InputStream in = IndexProvider.class.getResourceAsStream("/customer.json"); // todo: configurable
            byte[] bytes = IOUtils.toByteArray(in);
            mappings.put("customer", new String(bytes));

            in = IndexProvider.class.getResourceAsStream("/house.json");
            bytes = IOUtils.toByteArray(in);
            mappings.put("house", new String(bytes));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public IndexProvider(String date) throws IOException {
        idxMgr = new IndexManager(new HashMap<String, String>());
        String suffix = DateUtil.alignedByWeek(date);

        String newCustIdxName = String.format("%s_%s", "customer", suffix);
        if (!idxMgr.createIfNotExists(newCustIdxName, mappings.get("customer"))) {
            throw new IOException(String.format("create idx: %s failed.", newCustIdxName));
        }

        custIdxName = newCustIdxName;

        String newHouseIdxName = String.format("%s_%s", "house", suffix);
        if (!idxMgr.createIfNotExists(newHouseIdxName, mappings.get("house"))) {
            throw new IOException(String.format("create idx: %s failed.", newHouseIdxName));
        }

        houseIdxName = newHouseIdxName;
    }

    public String customerIdx() {
        return custIdxName;
    }

    public String houseIdx() {
        return houseIdxName;
    }
}
