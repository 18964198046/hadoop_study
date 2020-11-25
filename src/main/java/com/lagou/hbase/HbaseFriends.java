package com.lagou.hbase;

import com.sun.tools.javac.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HbaseFriends {

    Configuration conf = null;
    Connection conn = null;
    HBaseAdmin admin = null;

    @Before
    public void init () throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","linux121,linux122");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conn = ConnectionFactory.createConnection(conf);
        System.out.println(conn);
    }

    @Test
    public void createTable() throws IOException {
        admin = (HBaseAdmin) conn.getAdmin();
        HTableDescriptor work = new HTableDescriptor(TableName.valueOf("friend"));
        work.addFamily(new HColumnDescriptor("friends"));
        admin.createTable(work);
        System.out.println("friend表创建成功!!");
    }

    @Test
    public void putData() throws IOException {
        Put put1 = new Put(Bytes.toBytes("uid1"));
        put1.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid2"), Bytes.toBytes("uid2"));
        put1.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid5"), Bytes.toBytes("uid5"));
        put1.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid7"), Bytes.toBytes("uid7"));

        Put put2 = new Put(Bytes.toBytes("uid2"));
        put2.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid1"), Bytes.toBytes("uid1"));
        put2.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid3"), Bytes.toBytes("uid3"));
        put2.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid6"), Bytes.toBytes("uid6"));
        put2.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid10"), Bytes.toBytes("uid10"));

        Table table = conn.getTable(TableName.valueOf("friend"));
        table.put(List.of(put1, put2));
        table.close();
        System.out.println("插⼊入成功!!");
    }

    @Test
    public void deleteData() throws IOException {
        final Delete delete1 = new Delete(Bytes.toBytes("uid1"));
        delete1.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid2"));
        final Delete delete2 = new Delete(Bytes.toBytes("uid2"));
        delete2.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid1"));
        final Table table = conn.getTable(TableName.valueOf("friend"));
        table.delete(delete1);
        table.delete(delete2);
        table.close();
        System.out.println("删除数据成功!!");
    }

    @Test
    public void scanAllData() throws IOException {
        HTable table = (HTable) conn.getTable(TableName.valueOf("friend"));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(rowkey + "----" + family + "--" + column + "---" + value);
            }
        }
        table.close();
    }

    @Test
    public void deleteTable() throws IOException {
        admin = (HBaseAdmin) conn.getAdmin();
        admin.disableTable(Bytes.toBytes("friend"));
        admin.deleteTable(Bytes.toBytes("friend"));
        System.out.println("friend表删除成功!!");
    }

    @After
    public void destroy() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
