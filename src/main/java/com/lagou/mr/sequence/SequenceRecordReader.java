package com.lagou.mr.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SequenceRecordReader extends RecordReader<Text, BytesWritable> {

    private FileSplit fileSplit;
    private Configuration configuration;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private boolean isProgress = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit)inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress) {
            final Path path = fileSplit.getPath();
            final FileSystem fileSystem = path.getFileSystem(configuration);
            try (FSDataInputStream input = fileSystem.open(path)) {
                byte[] contents = new byte[(int)fileSplit.getLength()];
                IOUtils.readFully(input, contents, 0, contents.length);
                System.out.println("文件名:" + fileSplit.getPath().toString());
                key.set(fileSplit.getPath().toString());
                value.set(contents, 0, contents.length);
                isProgress = false;
                return true;
            }
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

}
