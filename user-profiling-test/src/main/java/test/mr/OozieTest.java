package test.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */

public class OozieTest {
    static class MapperTest extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(((FileSplit) context.getInputSplit()).getPath().toString()));
        }
    }
}
