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
            FileOutputStream fileOut = new FileOutputStream("employee.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(counter);
            out.close();
            fileOut.close();
            System.out.printf("Serialized data is saved in employee.ser");
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    @Test
    public void testDe() {
        UserPreference e = null;
        CountingBloomFilter counter = null;
        try
        {
            FileInputStream fileIn = new FileInputStream("employee.ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            counter = (CountingBloomFilter) in.readObject();
            in.close();
            fileIn.close();
        }catch(IOException i)
        {
            i.printStackTrace();
            return;
        }catch(ClassNotFoundException c)
        {
            System.out.println("Employee class not found");
            c.printStackTrace();
            return;
        }
        System.out.println("Deserialized Employee...");
    }

    @Test
    public void testX() {

        Object[] x = (Object[]) new Integer[]{1,2};
    }

}
