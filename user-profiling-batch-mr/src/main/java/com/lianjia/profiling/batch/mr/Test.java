package com.lianjia.profiling.batch.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */

public class Test {
    public static class TestMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            MapWritable doc = new MapWritable();
            doc.put(new Text("f1"), new Text("xxx"));
            context.write(NullWritable.get(), doc);
        }
    }

    public int bootstrap(String nodes, String input) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("es.nodes", nodes);
        conf.set("es.resource", "test/test");
        conf.set("mapred.map.tasks.speculative.execution", "false");
        conf.set("mapred.reduce.tasks.speculative.execution", "false");
        Job job = Job.getInstance(conf, "test");
        job.setJarByClass(Test.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setMapperClass(TestMapper.class);
        FileInputFormat.addInputPaths(job, input);

        if (job.waitForCompletion(true)) {
            return 0;
        } else {
            return 1;
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        new Test().bootstrap(args[0], args[1]); // "172.30.17.1:9200,172.30.17.2:9200"
    }
}
