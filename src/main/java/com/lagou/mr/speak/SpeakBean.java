package com.lagou.mr.speak;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SpeakBean implements Writable {

    private String deviceId;
    private long selfDuration;
    private long thirdDuration;
    private long totalDuration;

    public SpeakBean() {
    }

    public SpeakBean(String deviceId, long selfDuration, long thirdDuration) {
        this.deviceId = deviceId;
        this.selfDuration = selfDuration;
        this.thirdDuration = thirdDuration;
        this.totalDuration = selfDuration + thirdDuration;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public long getThirdDuration() {
        return thirdDuration;
    }

    public void setThirdDuration(long thirdDuration) {
        this.thirdDuration = thirdDuration;
    }

    public long getTotalDuration() {
        return totalDuration;
    }

    public void setTotalDuration(long totalDuration) {
        this.totalDuration = totalDuration;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(deviceId);
        dataOutput.writeLong(selfDuration);
        dataOutput.writeLong(thirdDuration);
        dataOutput.writeLong(totalDuration);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.deviceId = dataInput.readUTF();
        this.selfDuration = dataInput.readLong();
        this.thirdDuration = dataInput.readLong();
        this.totalDuration = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "SpeakBean{" +
                "deviceId='" + deviceId + '\'' +
                ", selfDuration=" + selfDuration +
                ", thirdDuration=" + thirdDuration +
                ", totalDuration=" + totalDuration +
                '}';
    }
}
