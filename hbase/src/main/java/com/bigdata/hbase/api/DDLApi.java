package com.bigdata.hbase.api;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.After;

import java.io.IOException;

public class DDLApi {
    private static HBaseConfiguration entries = new HBaseConfiguration();
    private static HBaseAdmin baseAdmin;
    private static HTable hTable;
    private static Connection connection;


    public static void main(String[] args) throws Exception {
        isTableExist("stu");
        createTable("stu", "info1", "info2");
        isTableExist("stu");
    }


    public static void startUp() throws Exception {
        entries.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
        entries.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(entries);
        baseAdmin = (HBaseAdmin) connection.getAdmin();
    }

    @After
    public void closeAdmin() throws IOException {
        System.out.println("进入了close的方法！！！");
        baseAdmin.close();
        if (connection != null) {
            connection.close();
        }
    }


    /**
     * 删除表
     *
     * @param tableName
     * @throws Exception
     */
    public void delTable(String tableName) throws Exception {
        startUp();
        boolean b = isTableExist(tableName);
        if (b) {
            baseAdmin.disableTable(TableName.valueOf(tableName));
            baseAdmin.deleteTable(TableName.valueOf(tableName));
        } else {
            System.out.println("不存在");
        }
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param args      ==========可变形参 必须要有列族
     * @throws Exception
     */
    public static void createTable(String tableName, String... args) throws Exception {
        startUp();
        if (args.length < 1) {
            return;
        }
        boolean b = isTableExist(tableName);
        if (!b) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String arg : args) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(arg);
                hColumnDescriptor.setMaxVersions(5);//指定最多有几个版本
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            baseAdmin.createTable(hTableDescriptor);
            System.out.println("=====成功=====");
        } else {
            System.out.println("=====失败=====");
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        HBaseConfiguration entries = new HBaseConfiguration();
        entries.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
        entries.set("hbase.zookeeper.property.clientPort", "2181");
        HBaseAdmin baseAdmin = new HBaseAdmin(entries);
        boolean b1 = baseAdmin.tableExists(tableName);
        System.out.println(b1);
        baseAdmin.close();
        return b1;
    }

}
