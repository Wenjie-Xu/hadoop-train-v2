package com.imooc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.nio.file.Files;

/**
 * 使用Java Api操作HDFS文件系统
 * 1、创建configuration
 * 2、获取filesystem
 * 3、调用Api
 */

public class HDFSApp {
    public static void main(String[] args) throws Exception{
    Configuration conf = new Configuration();

    URI uri = new URI("hdfs://hadoop000:8020");
    FileSystem filesystem = FileSystem.get(uri,conf,"apple");

    Path path = new Path("/hdfsapi/test");
    boolean result = filesystem.mkdirs(path);
    }
}
