package com.imooc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.jvm.hotspot.oops.ExceptionTableElement;
import sun.nio.ch.IOUtil;

import java.io.*;
import java.net.URI;

public class HDFSAPI {
    public static final String HDFS_URI = "hdfs://hadoop000:8020";
    FileSystem filesystem = null;
    Configuration conf = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("setUp----");
        conf = new Configuration();
        conf.set("dfs.replication", "1");
        filesystem = FileSystem.get(new URI(HDFS_URI), conf, "apple");
    }

    /**
     * 创建hdfs文件夹
     */
    @Test
    public void mkdir() throws Exception {
        filesystem.mkdirs(new Path("/hdfsjunit"));
    }

    /**
     * 查看目标文件夹所有文件
     */
    @Test
    public void listFile() throws Exception {
        FileStatus[] status = filesystem.listStatus(new Path("/"));
        for (FileStatus file : status) {
            String isdir = file.isDirectory() ? "文件夹" : "文件";
            String permision = file.getPermission().toString(); // 权限
            short replication = file.getReplication(); // 副本数
            long length = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isdir + "\t" + path + "\t" + replication
                    + "\t" + permision + "\t" + length);
        }
    }

    /**
     * 递归查看文件夹里的文件
     */
    @Test
    public void listRecursive() throws Exception {
        RemoteIterator<LocatedFileStatus> iterators = filesystem.listFiles(new Path("/hdfsapp"), true);
        while (iterators.hasNext()) {
            LocatedFileStatus file = iterators.next();
            String isdir = file.isDirectory() ? "文件夹" : "文件";
            String permision = file.getPermission().toString(); // 权限
            short replication = file.getReplication(); // 副本数
            long length = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isdir + "\t" + path + "\t" + replication
                    + "\t" + permision + "\t" + length);
        }
    }

    /**
     * 查看文件内容
     */
    @Test
    public void catFile() throws Exception {
        FSDataInputStream in = filesystem.open(new Path("/slaves"));
        IOUtils.copyBytes(in, System.out, 1024);
    }

    /**
     * 查看文件块内容
     */
    @Test
    public void getFileBlock() throws Exception {
        FileStatus file = filesystem.getFileStatus(new Path("/slaves"));
        // 获取所有机器上的文件块
        BlockLocation[] blocks = filesystem.getFileBlockLocations(file, 0, file.getLen());
        for (BlockLocation block : blocks) {
            // 一台机器上好多块
            for (String name : block.getNames()) {
                //偏移起始位置，文件块占用长度
                System.out.println(name + ":" + block.getOffset() + ":" + block.getLength());
            }
        }
    }

    /**
     * 创建文件
     */
    @Test
    public void createFile() throws Exception {
        FSDataOutputStream out = filesystem.create(new Path("/aa.txt"));
        out.writeUTF("Hello wenjie");
        out.flush();
        out.close();
    }

    /**
     * 文件名更改
     */
    @Test
    public void rename() throws Exception {
        Path old = new Path("/aa.txt");
        Path newPath = new Path("/bb.txt");
        boolean isdone = filesystem.rename(old, newPath);
        System.out.println(isdone);
    }

    /**
     * 拷贝本地文件到hdfs
     */
    @Test
    public void copyFromLocal() throws Exception {
        Path src = new Path("/Users/apple/Downloads/Hadoop2.6.0/etc/hadoop/core-site.xml");
        Path dest = new Path("/cc.txt");
        filesystem.copyFromLocalFile(src, dest);
    }

    /**
     * 拷贝大文件到hdfs
     */
    @Test
    public void copyBigFileFromLocal() throws Exception {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("/Users/apple/Downloads/Pandas2.pdf")));
        FSDataOutputStream out = filesystem.create(new Path("/hdfsjunit/Pandas.pdf"), new Progressable() {
            @Override
            public void progress() {
                System.out.print('.');
            }
        });
        IOUtils.copyBytes(in, out, 100);
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

    /**
     * 删除文件
     */
    @Test
    public void deleteFile() throws Exception {
        Boolean isdone = filesystem.delete(new Path("/a.zip"), true);
        System.out.println(isdone);
    }

    @After
    public void tearDown() {
        System.out.println("tearDown----");
        conf = null;
        filesystem = null;
    }
}
