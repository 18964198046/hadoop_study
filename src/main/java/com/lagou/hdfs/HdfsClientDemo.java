package com.lagou.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class HdfsClientDemo {

    private FileSystem fs;
    private Configuration conf;

    @Before
    public void init() throws IOException {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://linux121:9000");
        //conf.set("dfs.replication", "2"); //修改文件的副本数量
        fs = FileSystem.get(conf);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void hdfs_mkdir() throws IOException {
        fs.mkdirs(new Path("/speakInput"));
        fs.mkdirs(new Path("/speakOutput"));
    }

    @Test
    public void hdfs_copy_from_local() throws IOException {
        fs.copyFromLocalFile(new Path("/Users/yufangxing/Desktop/speak.data"), new Path("/speakInput/speak.data"));
    }

    @Test
    public void hdfs_copy_to_local() throws IOException {
        fs.copyToLocalFile(new Path("/api_test"), new Path("/Users/yufangxing/Desktop/Flink基础教程2.pdf"));
    }

    @Test
    public void hdfs_delete() throws IOException {
        fs.delete(new Path("/api_test"), true);
    }

    @Test
    public void hdfs_list() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            final String fileName = fileStatus.getPath().getName();
            final long len = fileStatus.getLen();
            final FsPermission permission = fileStatus.getPermission();
            final String group = fileStatus.getGroup();
            final String owner = fileStatus.getOwner();

            System.out.println(fileName + "\t" + len + "\t" + permission + "\t" + group + "\t" + owner);

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("主机名称" + host);
                }
            }

        }
    }

    @Test
    public void hdfs_is_file() throws IOException {
        final FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            final boolean isFile = fileStatus.isFile();
            if (isFile) {
                System.out.println("文件:" + fileStatus.getPath().getName());
            } else {
                System.out.println("文件夹:" + fileStatus.getPath().getName());
            }
        }
    }

    @Test
    public void hdfs_io_upload_file() throws IOException {
        File file = new File("/Users/yufangxing/Desktop/Flink基础教程.pdf");
        final FileInputStream inputStream = new FileInputStream(file);
        final FSDataOutputStream outputStream = fs.create(new Path("/api_io_test/Flink基础教程.pdf"));
        IOUtils.copyBytes(inputStream, outputStream, conf);
    }

    @Test
    public void hdfs_io_download_file() throws IOException {
        File file = new File("/Users/yufangxing/Desktop/Flink基础教程2.pdf");
        final FSDataInputStream inputStream = fs.open(new Path("/api_io_test/Flink基础教程.pdf"));
        final FileOutputStream outputStream = new FileOutputStream(file);
        IOUtils.copyBytes(inputStream, outputStream, conf);
    }

    @Test
    public void hdfs_io_seek_read() throws IOException {
        final FSDataInputStream inputStream = fs.open(new Path("/wcoutput/part-r-00000"));
        IOUtils.copyBytes(inputStream, System.out, 4096, false);
        inputStream.seek(0);
        System.out.println("-------再次输出---------");
        IOUtils.copyBytes(inputStream, System.out, 4096, true);
    }

}