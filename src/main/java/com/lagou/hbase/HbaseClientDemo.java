package com.lagou.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HbaseClientDemo {

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
        //创建表描述器器
        HTableDescriptor work = new HTableDescriptor(TableName.valueOf("teacher"));
        //设置列列族描述器器
        work.addFamily(new HColumnDescriptor("info"));
        //执⾏行行创建操作
        admin.createTable(work);
        System.out.println("teacher表创建成功!!");
    }

    //插⼊入⼀一条数据 @Test
    @Test
    public void putData() throws IOException {
        //设定rowkey
        Put put = new Put(Bytes.toBytes("110"));
        //列列族，列列，value
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("addr"), Bytes.toBytes("beijing"));
        //获取⼀一个表对象
        Table table = conn.getTable(TableName.valueOf("teacher"));
        //执⾏行行插⼊入
        table.put(put);
        //关闭table对象
        table.close();
        System.out.println("插⼊入成功!!");
    }

    //删除⼀一条数据
    @Test
    public void deleteData() throws IOException {
        //需要获取⼀一个table对象
        final Table worker = conn.getTable(TableName.valueOf("teacher"));
        //准备delete对象, 传入rowKey
        final Delete delete = new Delete(Bytes.toBytes("110"));
        //执⾏行行删除
        worker.delete(delete); //关闭table对象
        worker.close();
        System.out.println("删除数据成功!!");
    }

    //查询某个列列族数据
    @Test
    public void getDataByCF() throws IOException {
        //获取表对象
        HTable teacher = (HTable) conn.getTable(TableName.valueOf("teacher"));
        //创建查询的get对象
        Get get = new Get(Bytes.toBytes("110"));
        //指定列列族信息
        get.addFamily(Bytes.toBytes("info"));
        //执⾏行行查询
        Result res = teacher.get(get);
        //获取改⾏行行的所有cell对象
        Cell[] cells = res.rawCells();
        //通过cell获取rowkey,cf,column,value
        for (Cell cell : cells) {
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
            System.out.println(rowkey + "----" + cf + "---" + column + "---" + value);
        }
        //关闭表对象资源
        teacher.close();
    }

    /**
     * 全表扫描
     */
    @Test
    public void scanAllData() throws IOException {
        HTable teacher = (HTable) conn.getTable(TableName.valueOf("teacher"));
        Scan scan = new Scan();
        ResultScanner resultScanner = teacher.getScanner(scan);
        for (Result result : resultScanner) {
            //获取改⾏行行的所有cell对象
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //通过cell获取rowkey,cf,column,value
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                System.out.println(rowkey + "----" + cf + "--" + column + "---" + value);
            }
        }
        teacher.close();
    }

    /**
     * 通过startRowKey和endRowKey进⾏行行扫描查询 */
    @Test
    public void scanRowKey() throws IOException {
        HTable teacher = (HTable) conn.getTable(TableName.valueOf("teacher"));
        Scan scan = new Scan(); scan.setStartRow("0001".getBytes()); scan.setStopRow("2".getBytes());
        ResultScanner resultScanner = teacher.getScanner(scan); for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();//获取改⾏行行的所有cell对象
            for (Cell cell : cells) {
                //通过cell获取rowkey,cf,column,value
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                System.out.println(rowkey + "----" + cf + "--" + column + "---" + value);
            }
        }
        teacher.close();
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
