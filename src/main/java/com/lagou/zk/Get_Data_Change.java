package com.lagou.zk;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class Get_Data_Change {

    public static void main(String[] args) throws InterruptedException {

        String path = "/lg-zkClient-Ep";
        ZkClient zkClient = new ZkClient("linux121:2181");

        zkClient.setZkSerializer(new ZkSerializer() {
            @Override
            public byte[] serialize(Object data) throws ZkMarshallingError {
                return String.valueOf(data).getBytes();
            }

            @Override
            public Object deserialize(byte[] bytes) throws ZkMarshallingError {
                return String.valueOf(bytes);
            }
        });

        if(!zkClient.exists(path)) {
            zkClient.createEphemeral("/lg-client1", "123");
        }

        zkClient.subscribeDataChanges("/lg-client1", new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                System.out.println(path + "data is changed, new data " + data);
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                System.out.println(path + "data is deleted!");
            }
        });

        final Object data = zkClient.readData("/lg-client1");
        System.out.println(String.valueOf(data).getBytes());

        zkClient.writeData("/lg-client1", "new data");
        Thread.sleep(1000);

        zkClient.delete("/lg-client1");
        Thread.sleep(Integer.MAX_VALUE);
    }

}
