package com.lagou.zk;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;

public class Get_Child_Change {

    public static void main(String[] args) throws InterruptedException {
        final ZkClient zkClient = new ZkClient("linux121:2181");
        zkClient.subscribeChildChanges("/lg-client", new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                System.out.println(parentPath + "childs changes, current childs " + currentChilds);
            }
        });
        zkClient.createPersistent("/lg-client", true);
        Thread.sleep(1000);
        zkClient.createPersistent("/lg-client/c1", true);
        Thread.sleep(1000);
        zkClient.delete("/lg-client/c1");
        Thread.sleep(1000);
        zkClient.delete("/lg-client");
        Thread.sleep(100000);
    }

}
