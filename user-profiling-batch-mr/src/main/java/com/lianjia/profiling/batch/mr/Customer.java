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

public class Customer {
    public static class CustomerMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {
        private static final Logger LOG = Logger.getLogger(CustomerMapper.class);

        private static final int NUM_FIELDS = 6; // todo: 这个字段用不用加？从hive查出来是不是就可以保证？不过这个会导致下表越界。。

        /**
         * todo: 按时间戳滤掉未变化的，增量跟新，减小es压力。
         * todo: log格式？hadoop默认？把log4j配置到stdout？
         * todo: 加counters, 如处理失败行数
         * todo: 处理下conditional修改，有什么方便的方法？
         *
         * todo: snapshot
         */
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
                .addText("cust_id", fields[0])
                .addText("city_id", fields[1])
                .addText("app_id", fields[2])
                .addText("phone", fields[3])
                .getDoc();

            context.write(NullWritable.get(), doc);
        }
    }

    public int bootstrap(String nodes, String input) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("es.nodes", nodes);
        conf.set("es.resource", "customer/customer");
        conf.set("mapred.map.tasks.speculative.execution", "false");
        conf.set("mapred.reduce.tasks.speculative.execution", "false");
        conf.set("es.mapping.id", "cust_id");
        Job job = Job.getInstance(conf, "customer_test");
        job.setJarByClass(Customer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setMapperClass(CustomerMapper.class);
        FileInputFormat.addInputPaths(job, input);

        if (job.waitForCompletion(true)) {
            return 0;
        } else {
            return 1;
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        new Customer().bootstrap(args[0], args[1]); // "172.30.17.1:9200,172.30.17.2:9200"
    }
}
