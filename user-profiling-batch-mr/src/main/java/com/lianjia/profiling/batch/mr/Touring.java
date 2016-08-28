package com.lianjia.profiling.batch.mr;

import com.lianjia.profiling.batch.util.DocBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;

/**
 * @author fenglei@lianjia.com on 2016-03.
 */

public class Touring {
    public static class TouringMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {
        private static final Logger LOG = Logger.getLogger(TouringMapper.class);

        private static final int NUM_FIELDS = 11;

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\t");
            if (fields.length != NUM_FIELDS) {
                LOG.warn(String.format("fields.length != %d, line: %s", NUM_FIELDS, value.toString()));
                System.out.println(String.format("fields.length != %d, line: %s", NUM_FIELDS, value.toString()));
                return;
            }

            MapWritable doc = DocBuilder.newDoc()
                .addLong("touring_id", fields[0])
                .addText("cust_id", fields[1])
                .addLong("broker_ucid", fields[2])
                .addText("broker_code", fields[3])
                .addDate("touring_date", fields[4])
                .addText("feedback", fields[5])
                .addLong("biz_type", fields[6])
                .addLong("app_id", fields[7])
                .addLong("city_id", fields[8])
                .getDoc();

            context.write(NullWritable.get(), doc);
        }
    }

    public int bootstrap(String nodes, String input) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("es.nodes", nodes);
        conf.set("es.resource", "touring/touring");
        conf.set("mapred.map.tasks.speculative.execution", "false");
        conf.set("mapred.reduce.tasks.speculative.execution", "false");
        conf.set("es.mapping.id", "touring_id");
        Job job = Job.getInstance(conf, "touring_test");
        job.setJarByClass(Delegation.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setMapperClass(TouringMapper.class);
        FileInputFormat.addInputPaths(job, input);

        if (job.waitForCompletion(true)) {
            return 0;
        } else {
            return 1;
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        new Touring().bootstrap(args[0] ,args[1]); // "172.30.17.1:9200,172.30.17.2:9200"
    }
}
