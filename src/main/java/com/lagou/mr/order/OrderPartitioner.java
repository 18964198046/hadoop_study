package com.lagou.mr.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {

  @Override
  public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numOfPartitions) {
     return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numOfPartitions;
  }

}