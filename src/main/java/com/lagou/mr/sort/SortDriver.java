package com.lagou.mr.sort;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortDriver {

    /**
     * file文件和执行结果保存在了resources/work目录下，
     * 因为不知道怎么指定resource目录作为Path，
     * 因此直接在本地环境执行完成后，将结果放在了resource目录下
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, new Path("/Users/yufangxing/Desktop/data/work1/"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/yufangxing/Desktop/data/work1/out"));
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : -1);
    }

}
