package com.lagou.mr.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutputFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        final FileSystem fileSystem = FileSystem.get(configuration);
        final Path lagouPath = new Path("/Users/yufangxing/Desktop/data/click_log/out/lagou.log");
        final Path otherPath = new Path("/Users/yufangxing/Desktop/data/click_log/out/other.log");
        final FSDataOutputStream lagouOut = fileSystem.create(lagouPath);
        final FSDataOutputStream otherOut = fileSystem.create(otherPath);
        return new OutputRecordWriter(lagouOut, otherOut);
    }

}
