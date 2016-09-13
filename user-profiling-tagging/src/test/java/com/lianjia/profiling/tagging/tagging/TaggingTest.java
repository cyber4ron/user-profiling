package com.lianjia.profiling.tagging.tagging;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lianjia.profiling.tagging.features.Features;
import com.lianjia.profiling.tagging.features.UserPreference;
import com.lianjia.profiling.tagging.tag.UserTag;
import com.lianjia.profiling.tagging.user.OfflineEventTagging;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class TaggingTest {

    @Test
    public void test() {
        Class<Features.AreaRange> xx = Features.AreaRange.class;
        boolean x = Features.AreaRange.class.isAssignableFrom(xx);
        boolean y = Features.PriceRangeLv1.class.isAssignableFrom(xx);
        System.out.println();
    }

    @Test
    public void testCompute() throws Exception {
        String json = new String(Files.readAllBytes(Paths.get("src/main/resources/user_offline.json")));

        JSONObject offline = JSON.parseObject(json);

        // System.out.println(Tagging.PriceRangeLv1.idx(9700000));

        UserPreference prefer = OfflineEventTagging.compute((JSONObject) offline.get("data"));

        System.out.println(prefer);

        System.out.println(prefer.toReadableJson());

        System.out.println();
    }

    @Test
    public void testCompute2() throws Exception {
        UserPreference prefer = new UserPreference();

        prefer.update(UserTag.RESBLOCK, "BJ456787654", 1.0F);
        prefer.update(UserTag.RESBLOCK, "BJ456787654", 1.0F);
        prefer.update(UserTag.RESBLOCK, "BJ456787654", 1.0F);
        prefer.update(UserTag.RESBLOCK, "BJ456787654", 1.0F);

        // System.out.println(Arrays.toString(RunLengthEncoding.deserializeBase64(prefer.entries.get("resblock_cbf").toString())));
        System.out.println();
    }

    @Test
    public void testCompute3() throws Exception {
        UserPreference prefer = new UserPreference();

        prefer.update(UserTag.UNIQUE, 1, 1.0F);
        prefer.update(UserTag.UNIQUE, 1, 1.0F);
        prefer.update(UserTag.UNIQUE, 1, 1.0F);
        prefer.update(UserTag.UNIQUE, 1, 1.0F);

        // System.out.println(Arrays.toString(RunLengthEncoding.deserializeBase64(prefer.entries.get("resblock_cbf").toString())));
        System.out.println();
    }

    @Test
    public void testCompute4() throws Exception {
        for(Map.Entry<Integer, Double> e: Features.DECAY.entrySet()) {
            System.out.println(e.getKey() + ", "+ e.getValue());
        }
    }
}
