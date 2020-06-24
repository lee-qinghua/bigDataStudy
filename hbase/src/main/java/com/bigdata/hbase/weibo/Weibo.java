package com.bigdata.hbase.weibo;

import com.bigdata.hbase.weibo.Util.WeiBoUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 说明：微博内容表为t1 关系表为t2 收件箱表为t3
 */
public class Weibo {
    private static HBaseConfiguration entries = new HBaseConfiguration();
    private static HTable hTable;
    private static Connection connection;

    public static void startUp() throws IOException {
        entries.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
        entries.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(entries);
    }

    /**
     * 发微博
     */
    public void publishWeibo(String userId, String content) throws IOException {
        startUp();
        /**在weibo表插入数据*/
        hTable = (HTable) connection.getTable(TableName.valueOf("t1"));
        String rowKey = userId + "_" + System.currentTimeMillis();
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));
        hTable.put(put);
        /**获取粉丝列表*/
        HTable t2 = (HTable) connection.getTable(TableName.valueOf("t2"));
        Get get = new Get(Bytes.toBytes(userId));
        get.addFamily(Bytes.toBytes("fans"));
        Result result = t2.get(get);
        List<Cell> cells = result.listCells();
        /**在粉丝的首页表插入数据*/
        ArrayList<Put> list = new ArrayList<>();
        for (Cell cell : cells) {
            String fanId = Bytes.toString(CellUtil.cloneValue(cell));
            //把发微博人的最近三条微博的数据插入到fanid的收件箱中
            Put t3Put = new Put(Bytes.toBytes(fanId));
            t3Put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(userId), Bytes.toBytes(rowKey));
            list.add(t3Put);
        }
        if (list.size() > 0) {//对于没有粉丝的人值为0
            Table t3 = connection.getTable(TableName.valueOf("t3"));
            t3.put(list);
            t3.close();
        }
        //关闭资源
        t2.close();
        hTable.close();
        connection.close();
    }

    /**
     * 关注用户
     */
    public void fllowStart(String userId, String starId) throws IOException {
        startUp();
        /**在用户的关注表中添加记录*/
        HTable t2 = (HTable) connection.getTable(TableName.valueOf("t2"));
        Put put = new Put(Bytes.toBytes(userId));
        put.addColumn(Bytes.toBytes("attend"), Bytes.toBytes(starId), Bytes.toBytes(starId));
        t2.put(put);
        /**在被关注用户的粉丝表中添加记录*/
        Put starPut = new Put(Bytes.toBytes(starId));
        starPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(userId), Bytes.toBytes(userId));
        t2.put(starPut);
        /**在用户的收件箱表中添加数据*/
        //获取被关注者的最新三条微博,两种方案：
        // 1. 更改设计发微博时的rowkey 是userid_(9999...999-时间戳)，这样最新发布的微博排序在最上面，取的时候用scan 取前三条
        // 2. rowkey还用之前的设计，用scan扫出来，最新发布的微博在最下面。循环一直插入到t3表中，由于t3表设计的version是3个版本，所以只保留最新的三条数据。但是这个不靠谱，也太浪费资源了。
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(starId + "_"));
        scan.setStopRow(Bytes.toBytes(starId + "|"));
        HTable t1 = (HTable) connection.getTable(TableName.valueOf("t1"));
        ResultScanner results = t1.getScanner(scan);
        long l = System.currentTimeMillis();
        ArrayList<Put> puts = new ArrayList<>();
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                Put put1 = new Put(Bytes.toBytes(userId));
                put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes(starId), l++, CellUtil.cloneRow(cell));
                puts.add(put1);
            }
        }
        HTable t3 = (HTable) connection.getTable(TableName.valueOf("t3"));
        t3.put(puts);
    }

}
