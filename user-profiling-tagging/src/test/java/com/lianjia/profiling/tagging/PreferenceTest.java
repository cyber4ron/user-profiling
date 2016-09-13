package com.lianjia.profiling.tagging;

import com.lianjia.profiling.tagging.counter.CountingBloomFilter;
import com.lianjia.profiling.tagging.features.UserPreference;
import org.junit.Test;

import java.io.*;

/**
 * @author fenglei@lianjia.com on 2016-06
 */

public class PreferenceTest {

    @Test
    public void testCompute() throws Exception {
//        String[] arr = Preference.dedup(new String[]{"a", "b", "s", "a", "a", "t"});
//        System.out.println();
    }

    @Test
    public void testSer() {
        UserPreference preference = new UserPreference();
        CountingBloomFilter counter = new CountingBloomFilter(0, 0);
        try {
            FileOutputStream fileOut = new FileOutputStream("prefer_ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(preference);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    @Test
    public void testDe() {
        UserPreference e = null;
        CountingBloomFilter counter = null;
        try {
            FileInputStream fileIn = new FileInputStream("prefer_ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            e = (UserPreference) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException i) {
            i.printStackTrace();
            return;
        } catch (ClassNotFoundException c) {
            System.out.println("Employee class not found");
            c.printStackTrace();
            return;
        }
        System.out.println("Deserialized Employee...");
    }

    @Test
    public void textA() {
        int n = 200;
        double p = 0.05;
        double x = Math.max(1, (int) Math.ceil(-n * Math.log(p) / Math.pow(Math.log(2), 2)));
        System.out.println(x);

        // 工程上不是很划算的方法

        // 655.36
        // 1248 * 2 = 2500

        // 200 * (24 + 4) = 3200
    }

}
