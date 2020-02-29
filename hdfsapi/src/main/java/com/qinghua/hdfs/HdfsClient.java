package com.qinghua.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    public static void main(String[] args) throws IOException, InterruptedException {
        getInfo();
    }

    /**
     * 文件的基础操作
     */
    public void method() throws Exception, InterruptedException {
        //new Configuration 就是hadoop etc文件夹下的core-site.xml配置文件
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop101:9000"), configuration, "liqinghua");//以liqinghua这个用户登陆
        fileSystem.copyFromLocalFile(new Path("D:\\1.txt"), new Path("/"));
        //fileSystem.rename();重命名
        //fileSystem.append();追加
        //fileSystem.delete();删除
        fileSystem.close();
    }

    /**
     * 文件拷贝
     */
    public void copy() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "liqinghua");
        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/banzhang.txt"), new Path("e:/banhua.txt"), true);
        // 3 关闭资源
        fs.close();
    }

    /**
     * 文件详情查看
     */
    public static void getInfo() {

        try {
            // 1 获取文件系统
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "liqinghua");
            // true:子目录是否需要递归遍历
            // 有一个listStatus方法 这个方法文件和文件夹都能获取
            // listFiles只能获取文件
            RemoteIterator<LocatedFileStatus> fileStatus = fs.listFiles(new Path("/"), true);
            while (fileStatus.hasNext()) {
                LocatedFileStatus status = fileStatus.next();
                // 文件名称
                System.out.println(status.getPath().getName());
                // 长度
                System.out.println(status.getLen());
                // 权限
                System.out.println(status.getPermission());
                // 分组
                System.out.println(status.getGroup());
                // 获取文件的所有的块信息，可以找到每个块存储的ip地址
                BlockLocation[] blockLocations = status.getBlockLocations();
                for (BlockLocation block : blockLocations) {
                    String[] hosts = block.getHosts();
                    for (String host : hosts) {
                        System.out.println(host);
                    }
                }
            }
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 流操作：文件上传
     */
    public void method1() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "liqinghua");
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("e:/banhua.txt"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/banhua.txt"));

        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 流操作：文件下载
     */
    public void method2() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "liqinghua");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/banhua.txt"));

        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/banhua.txt"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }
}
