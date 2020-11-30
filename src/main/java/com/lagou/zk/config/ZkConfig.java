//package com.lagou.zk.config;
//
//import org.I0Itec.zkclient.IZkChildListener;
//import org.I0Itec.zkclient.ZkClient;
//import org.springframework.boot.ApplicationArguments;
//import org.springframework.boot.ApplicationRunner;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.stereotype.Component;
//
//import java.util.List;
//
//@Configuration
//public class ZkConfig {
//
//    @Bean
//    public ZkClient zkClient() {
//        ZkClient zkClient = new ZkClient("linux121:2181");
//        zkClient.subscribeChildChanges("/spring/datasource", new IZkChildListener() {
//            @Override
//            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
//                System.out.println(parentPath + "childs changes, current childs " + currentChilds);
//            }
//        });
//        return zkClient;
//    }
//
//
//}
