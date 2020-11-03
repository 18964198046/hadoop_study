package com.lagou.mr.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<IntWritable, NullWritable, Text, NullWritable> {

    private Integer rowNum = 0;

    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Text text = new Text( ++rowNum + "\t" + key.get());
        context.write(text, NullWritable.get());
    }

}
