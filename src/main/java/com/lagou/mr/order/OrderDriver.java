package com.lagou.mr.order;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration configuration = new Configuration();
        //configuration.set("fs.defaultFS", "hdfs://linux121:9000");
        final Job job = Job.getInstance(configuration, "OrderDriver");
        job.setJarByClass(OrderDriver.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(OrderBean.class);
        job.setPartitionerClass(OrderPartitioner.class);
        job.setGroupingComparatorClass(OrderGroupingComparator.class);
        job.setNumReduceTasks(3);
        FileInputFormat.setInputPaths(job, new Path("/Users/yufangxing/Desktop/data/GroupingComparator/groupingComparator.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/yufangxing/Desktop/data/GroupingComparator/out"));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : -1);
    }

}
