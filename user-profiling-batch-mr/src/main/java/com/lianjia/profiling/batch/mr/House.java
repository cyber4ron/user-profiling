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

public class House {
    public static class HouseMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {
        private static final Logger LOG = Logger.getLogger(HouseMapper.class);

        private static final int NUM_FIELDS = 38;

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
                .addText("house_id", fields[0])
                .addLong("hdic_id", fields[1])
                .addLong("creator_ucid", fields[2])
                .addLong("creator_id", fields[3])
                .addText("status", fields[4])
                .addDate("invalid_date", fields[5])
                .addLong("biz_type", fields[6])
                .addDouble("list_price", fields[7])
                .addLong("orient_id", fields[8])
                .addInt("bedr_num", fields[9])
                .addInt("livingr_num", fields[10])
                .addInt("kitchen_num", fields[11])
                .addInt("bathr_num", fields[12])
                .addInt("balcony_num", fields[13])
                .addDouble("area", fields[14])
                .addText("district_code", fields[15])
                .addText("district_name", fields[16])
                .addText("bizcircle_code", fields[17])
                .addText("bizcircle_name", fields[18])
                .addText("resblock_id", fields[19])
                .addText("resblock_name", fields[20])
                .addText("building_id", fields[21])
                .addText("building_name", fields[22])
                .addText("unit_code", fields[23])
                .addText("unit_name", fields[24])
                .addInt("floor_no", fields[25])
                .addText("house_usage_code", fields[26])
                .addText("house_usage_name", fields[27])
                .addInt("floors_num", fields[28])
                .addInt("completed_year", fields[29])
                .addText("has_sale_tax", fields[30])
                .addText("metro_dist_code", fields[31])
                .addText("metro_dist_name", fields[32])
                .addText("is_school_district", fields[33])
                .addText("fitment_type_code", fields[34])
                .addText("fitment_type_name", fields[35])
                .getDoc();

            context.write(NullWritable.get(), doc);
        }
    }

    public int bootstrap(String nodes, String input) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("es.nodes", nodes);
        conf.set("es.resource", "house/house");
        conf.set("mapred.map.tasks.speculative.execution", "false");
        conf.set("mapred.reduce.tasks.speculative.execution", "false");
        conf.set("es.mapping.id", "house_id");
        Job job = Job.getInstance(conf, "house_test");
        job.setJarByClass(Delegation.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setMapperClass(HouseMapper.class);
        FileInputFormat.addInputPaths(job, input);

        if (job.waitForCompletion(true)) {
            return 0;
        } else {
            return 1;
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        new House().bootstrap(args[0], args[1]); // "172.30.17.1:9200,172.30.17.2:9200"

        House h = new House();

    }
}
