package com.bigdata.hbase.api;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DMLApi {
    private static HBaseConfiguration entries = new HBaseConfiguration();
    private static Connection connection;

    public static void main(String[] args) throws Exception {
        //1 添加数据
        //addData("stu", "1002", "info1", "name", "wangwu");
        //addData("stu", "1003", "info2", "loc", "beijing");

        //2. 获取数据
        //getData("stu", "1001","info1", "name");

        //3. 获取所有数据
        scanData("stu");

        //4 删除数据
        delData("stu", "1001", "1002");
    }

    /**
     * 初始化
     */
    public static void startUp() {
        try {
            entries.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
            entries.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(entries);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 添加数据
     *
     * @param tableName  表名
     * @param rowKey
     * @param cf         列族
     * @param cloumnName
     * @param value
     * @throws Exception
     */
    public static void addData(String tableName, String rowKey, String cf, String cloumnName, String value) throws Exception {
        startUp();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cloumnName), Bytes.toBytes(value));
        table.put(put);
        table.close();
        connection.close();
    }

    /**
     * 删除多行数据
     *
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void delData(String tableName, String... rows) throws IOException {
        startUp();
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
        connection.close();
    }

    /**
     * scan 获取所有的值
     *
     * @param tableName
     * @throws IOException
     */
    public static void scanData(String tableName) throws IOException {
        startUp();
        Table table = connection.getTable(TableName.valueOf(tableName));
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
        connection.close();
    }

    /**
     * 获取数据
     *
     * @param tableName
     * @param rowkey
     * @param cf
     * @param cn
     */
    public static void getData(String tableName, String rowkey, String cf, String cn) throws IOException {
        startUp();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        get.setMaxVersions();
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
        connection.close();
    }
}
