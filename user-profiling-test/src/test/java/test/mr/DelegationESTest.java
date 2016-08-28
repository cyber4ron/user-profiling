package test.mr;

import com.lianjia.profiling.batch.mr.Delegation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */

public class DelegationESTest {
    MapDriver<LongWritable, Text, NullWritable, MapWritable> mapDriver;

    @Before
    public void setUp() throws UnknownHostException {
        Delegation.DelegationMapper mapper = new Delegation.DelegationMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("qqq"));
        // MapWritable output = (MapWritable) new MapWritable().put(new Text("f1"), new Text("v1"));
        // mapDriver.withOutput(NullWritable.get(), output);
        mapDriver.runTest();
    }
}
