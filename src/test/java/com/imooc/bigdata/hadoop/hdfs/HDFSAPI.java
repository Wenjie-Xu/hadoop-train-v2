package com.imooc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.jvm.hotspot.oops.ExceptionTableElement;
import sun.nio.ch.IOUtil;

import java.net.URI;

public class HDFSAPI {
    public static final String HDFS_URI = "hdfs://hadoop000:8020";
    FileSystem filesystem = null;
    Configuration conf = null;

    @Before
    public void setUp() throws Exception{
        System.out.println("setUp----");
        conf = new Configuration();
        conf.set("dfs.replication","1");
        filesystem = FileSystem.get(new URI(HDFS_URI), conf, "apple");
    }

    /**
     * 创建hdfs文件夹
     */
    @Test
    public void mkdir() throws Exception{
        filesystem.mkdirs(new Path("/hdfsjunit"));
    }
    /**
     * 查看目标文件夹所有文件
     */
    @Test
    public void listFile() throws Exception{
        FileStatus[] status= filesystem.listStatus(new Path("/"));
        for(FileStatus file : status) {
            String isdir = file.isDirectory() ? "文件夹" : "文件";
            String permision = file.getPermission().toString(); // 权限
            short replication = file.getReplication(); // 副本数
            long length = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isdir+"\t"+path+"\t"+replication
                                +"\t"+permision+"\t"+length);
        }
    }

    /**
     * 查看文件内容
     */
    @Test
    public void catFile() throws Exception{
        FSDataInputStream in = filesystem.open(new Path("/slaves"));
        IOUtils.copyBytes(in,System.out,1024);
    }

    /**
     * 创建文件
     */
    @Test
    public void createFile() throws Exception{
        FSDataOutputStream out = filesystem.create(new Path("/aa.txt"));
        out.writeUTF("Hello wenjie");
        out.flush();
        out.close();
    }

    /**
     * 文件名更改
     */
    @Test
    public void rename() throws Exception{
        Path old = new Path("/aa.txt");
        Path newPath = new Path("/bb.txt");
        boolean isdone = filesystem.rename(old,newPath);
        System.out.println(isdone);
    }

    /**
     * 拷贝本地文件到hdfs
     */
    @Test
    public void copyFromLocal() throws Exception{
        Path src = new Path("/Users/apple/Downloads/Hadoop2.6.0/etc/hadoop/core-site.xml");
        Path dest = new Path("/cc.txt");
        filesystem.copyFromLocalFile(src, dest);
    }

    /**
     * 将hdfs文件拷贝到本地
     */
    @Test
    public void copyToLocal() throws Exception {
        Path src = new Path("/cc.txt");
        Path dest = new Path("/Users/apple/Desktop/cc.txt");
        filesystem.copyToLocalFile(src, dest);
    }
    @After
    public void tearDown(){
        System.out.println("tearDown----");
        conf = null;
        filesystem = null;
    }
}
