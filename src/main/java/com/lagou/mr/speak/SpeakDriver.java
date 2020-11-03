package com.lagou.mr.speak;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SpeakDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration configuration = new Configuration();
        configuration.set("fs.defaultFs", "hdfs://linux121:9000");
        final Job job = Job.getInstance(configuration, "SpeakDriver");
        job.setJarByClass(SpeakDriver.class);
        job.setMapperClass(SpeakMapper.class);
        job.setReducerClass(SpeakReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SpeakBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SpeakBean.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        final boolean flag = job.waitForCompletion(true);
        System.out.println(flag ? 0 : -1);
    }

}
