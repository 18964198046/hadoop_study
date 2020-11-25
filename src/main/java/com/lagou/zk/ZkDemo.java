package com.lagou.zk;

import org.I0Itec.zkclient.ZkClient;

public class ZkDemo {

    public static void main(String[] args) {
        // 先获取到zkClient对象, client与zk集群的默认通信端口是2181
        final ZkClient zkClient = new ZkClient("linux121:2181", 1000);
        System.out.println("zkClient is ready");

        //createParents的值设置为true，可以递归创建节点
        zkClient.createPersistent("/lg-zkClient/lg-c1",true);
        System.out.println("success create znode.");
    }

}
