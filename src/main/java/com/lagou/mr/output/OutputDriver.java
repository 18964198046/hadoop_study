package com.lagou.mr.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutputDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);
        job.setMapperClass(OutputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(OutputReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(OutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("/Users/yufangxing/Desktop/data/click_log/click_log.data"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/yufangxing/Desktop/data/click_log/out"));
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : -1);
    }

}
