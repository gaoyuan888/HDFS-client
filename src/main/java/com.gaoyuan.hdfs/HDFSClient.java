package com.gaoyuan.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * 功能描述:
 *
 * @author yaoyizhou
 * @date 2020/1/27 9:44
 * @desc
 */
public class HDFSClient {

    public static void main(String[] args) {
//        test1();
        test2();
    }


    private static void test1() {
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();
        // 1.1 namenode 的地址
        configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // 2 获取文件系统
        FileSystem fs = null;
        try {
            fs = FileSystem.get(configuration);
            fs.mkdirs(new Path("/0529/dashen"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // 3 打印文件系统
        System.out.println("over");
    }


    private static void test2() {
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();
        // 1.1 namenode 的地址
        configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // 2 获取文件系统
        FileSystem fs = null;
        try {
            fs = FileSystem.get(configuration);

            FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

            fs.mkdirs(new Path("/0529/dashen/banzhang"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // 3 打印文件系统
        System.out.println("over");
    }


    //文件上传
    @Test
    public void testCopyFromLocalFile() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
        // 2 上传文件
        fs.copyFromLocalFile(new Path("e:/hello.txt"), new Path("/hello3.txt"));
        // 3 关闭资源
        fs.close();
        System.out.println("over");
    }


    //文件下载
    @Test
    public void testCopyToLocalFile() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
        // 2 下载文件
        fs.copyToLocalFile(false, new Path("/hello3.txt"), new Path("e://hello1.txt"));
        // 3 关闭资源
        fs.close();
        System.out.println("over");
    }


    //删除下载
    @Test
    public void testDelete() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
        // 2 删除文件
        fs.delete(new Path("/hello3.txt"), false);
        // 3 关闭资源
        fs.close();
        System.out.println("over");
    }


    @Test
    public void testRename() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
        // 2 修改文件名称
        fs.rename(new Path("/hello1.txt"), new Path("/hello11.txt"));
        // 3 关闭资源
        fs.close();
    }


    @Test
    public void testListFiles() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration,"root");
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            // 输出详情
            // 文件名称
            System.out.println("文件名称:"+status.getPath().getName());
            // 长度
            System.out.println("长度："+status.getLen());
            // 权限
            System.out.println("权限："+status.getPermission());
            // z 组
            System.out.println("组："+status.getGroup());
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.print(host+",");
                }
            }
            System.out.println();
            System.out.println("----------------班长的分割线-----------");
        }
    }


    @Test
    public void testListStatus() throws Exception{
        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration,"root");
        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            } }
        // 3 关闭资源
        fs.close();
    }


    //IO操作
    @Test
    public void putFileToHDFS() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration,
                "root");
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("e:/hello.txt"));
        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/hello4.txt"));
        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }


    @Test
    public void getFileFromHDFS() throws Exception{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration,"root");
        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hello4.txt"));

        // 3 输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hello33.txt"));
        // 4 获取输出流
        IOUtils.copyBytes(fis, fos, configuration);
        // 5 流对拷
        // 6 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }


    //下载大文件第一块
    @Test
    public void readFileSeek1() throws Exception{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration,"root");
        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new
                File("e:/hadoop-2.7.2.tar.gz.part1"));
        // 4 流的拷贝
        byte[] buf = new byte[1024];
        for(int i =0 ; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }
        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }


    @Test
    public void readFileSeek2() throws Exception{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration,"root");
        // 2 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        // 3 定位输入数据位置
        fis.seek(1024*1024*128);
        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new
                File("e:/hadoop-2.7.2.tar.gz.part2"));
        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }


}
