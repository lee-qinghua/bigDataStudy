package com.qinghua.hive.youtube统计项目;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * key为NullWritable就不会走reduce本来数据清洗也不需要reduce
 */
public class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * 目前数据的问题
         * 1. 有的数据可能字段不全（目前是10个字段，最后一个字段可以没有值），解析出的数据必须>=9个字段
         * 2. 分类字段是一个数组使用连接符&连接，但是目前有'\t'分隔符 a & b 是这样的，应该把他变得紧凑a&b
         * 3. 关联视频字段也是一个数组，但是目前没有连接符，增加成&连接符，建表时一个表中，一种类型只能用一种连接符。
         */
        String datas = value.toString();
        String[] fields = datas.split("\t");
        //1
        if (fields.length < 9) {
            return;
        }
        //2
        fields[3] = fields[3].replaceAll(" ", "");
        StringBuffer stringBuffer1 = new StringBuffer();
        if (fields.length == 9) {
            for (int i = 0; i < fields.length; i++) {
                if (i == 8) {
                    stringBuffer1.append(fields[i]);
                } else {
                    stringBuffer1.append(fields[i]).append("\t");
                }
            }
        } else {
            for (int i = 0; i < fields.length; i++) {
                if (i > 8) {
                    if (i == fields.length - 1) {
                        stringBuffer1.append(fields[i]);
                    } else {
                        stringBuffer1.append(fields[i]).append("&");
                    }
                } else {
                    stringBuffer1.append(fields[i]).append("\t");
                }

            }
        }
        String s = stringBuffer1.toString();
        text.set(s);
        context.write(NullWritable.get(), text);
    }
}
