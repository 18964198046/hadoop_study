package com.lagou.mr.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class OutputRecordWriter extends RecordWriter<Text, NullWritable> {

    private final FSDataOutputStream lagouOut;
    private final FSDataOutputStream otherOut;

    public OutputRecordWriter(FSDataOutputStream lagouOut, FSDataOutputStream otherOut) {
        this.lagouOut = lagouOut;
        this.otherOut = otherOut;
    }

    @Override
    public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
        String text = key.toString();
        if (text.contains("lagou")) {
            lagouOut.write(text.getBytes());
            lagouOut.write("\r\n".getBytes());
        } else {
            otherOut.write(text.getBytes());
            otherOut.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(lagouOut);
        IOUtils.closeStream(otherOut);
    }

}
