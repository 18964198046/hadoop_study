package com.lagou.mr.speak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpeakReducer extends Reducer<Text, SpeakBean, Text, SpeakBean> {

    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Context context) throws IOException, InterruptedException {
        long selfDuration = 0l;
        long thirdDuration = 0l;
        String deviceId = key.toString();
        for (SpeakBean speakBean : values) {
            selfDuration += speakBean.getSelfDuration();
            thirdDuration += speakBean.getThirdDuration();
        }
        SpeakBean speakBean = new SpeakBean(deviceId, selfDuration, thirdDuration);
        context.write(key, speakBean);
    }
}
