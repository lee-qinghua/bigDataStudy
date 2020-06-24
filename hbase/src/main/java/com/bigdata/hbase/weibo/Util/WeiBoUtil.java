package com.bigdata.hbase.weibo.Util;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class WeiBoUtil {
    private static HBaseConfiguration entries = new HBaseConfiguration();
    private static HBaseAdmin baseAdmin;
    private static HTable hTable;
    private static Connection connection;

    //2 创建表
    public static void createTable(String tableName, int versions, String... cfs) throws IOException {
        boolean exist = isTableExist(tableName);
        if (exist) {
            System.out.println("com.bigdata.hbase.weibo.Util.WeiBoUtil.createTable:表名已存在！");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : cfs) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                hColumnDescriptor.setMaxVersions(versions);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            baseAdmin.createTable(hTableDescriptor);
            baseAdmin.close();
            connection.close();
            System.out.println("com.bigdata.hbase.weibo.Util.WeiBoUtil.createTable:创建表成功!");
        }
    }

    //3 判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {
        boolean b = baseAdmin.tableExists(TableName.valueOf(tableName));
        baseAdmin.close();
        connection.close();
        return b;
    }

    public static void startUp() throws IOException {
        entries.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
        entries.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(entries);
        baseAdmin = (HBaseAdmin) connection.getAdmin();
    }
}
