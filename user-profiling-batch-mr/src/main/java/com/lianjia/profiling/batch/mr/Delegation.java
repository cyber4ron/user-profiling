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

public class Delegation {
    public static class DelegationMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {
        private static final Logger LOG = Logger.getLogger(DelegationMapper.class);

        private static final int NUM_FIELDS = 19;

        /**
         * todo: 应该是只能添加和更新？测下
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\t");
            if (fields.length != NUM_FIELDS) {
                LOG.warn(String.format("fields.length != %d, line: %s", NUM_FIELDS, value.toString()));
                System.out.println(String.format("fields.length != %d, line: %s", NUM_FIELDS, value.toString()));
                return;
            }

            MapWritable doc = DocBuilder.newDoc()
                // .addText("uuid", UUID.randomUUID().toString())
                .addText("cust_id", fields[0])
                .addDate("creation_date", fields[1])
                .addDate("invalid_date", fields[2])
                .addDate("update_date", fields[3])
                .addText("creation_ucid", fields[4])
                .addText("creation_code", fields[5])
                .addText("invalid_ucid", fields[6])
                .addText("update_ucid", fields[7])
                .addText("update_code", fields[8])
                .addLong("biz_type", fields[9])
                .addText("district_name", fields[10])
                .addText("bizcircle_name", fields[11])
                .addInt("room_min", fields[12])
                .addInt("room_max", fields[13])
                .addDouble("area_min", fields[14])
                .addDouble("area_max", fields[15])
                .addInt("balcony_need", fields[16])
                .getDoc();

            context.write(NullWritable.get(), doc);
        }
    }

    /**
     * todo: oozie里有什么方便穿参数的方式？
     */
    public int bootstrap(String nodes, String input) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("es.nodes", nodes);
        conf.set("es.resource", "delegation2/delegation");
        conf.set("mapreduce.map.speculative", "false");
        conf.set("mapreduce.reduce.speculative", "false");
        // conf.set("es.mapping.id", "uuid");
        Job job = Job.getInstance(conf, "delegation_test");
        job.setJarByClass(Delegation.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setMapperClass(DelegationMapper.class);
        FileInputFormat.addInputPaths(job, input);

        if (job.waitForCompletion(true)) {
            return 0;
        } else {
            return 1;
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        new Delegation().bootstrap(args[0] ,args[1]); // "172.30.17.1:9200,172.30.17.2:9200"
        System.out.println("xx");
    }
}
