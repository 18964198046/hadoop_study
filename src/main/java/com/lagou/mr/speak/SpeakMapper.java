package com.lagou.mr.speak;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeakMapper extends Mapper<LongWritable, Text, Text, SpeakBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        SpeakBean speakBean = new SpeakBean();
        speakBean.setDeviceId(fields[1]);
        speakBean.setSelfDuration(Long.valueOf(fields[4]));
        speakBean.setThirdDuration(Long.valueOf(fields[5]));
        context.write(new Text(fields[1]), speakBean);
    }

}
