package com.qinghua.mapreduce.自定义排序;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 实现按照总流量倒序排序，bean对象作为key
 */
public class MyFlowDriver {
    public static void main(String[] args) throws Exception {
        //1 获取一个job实例
        Job job = Job.getInstance();
        //2 设置类的路径
        job.setJarByClass(MyFlowDriver.class);
        //3 设置mapper和reduce的路径
        job.setMapperClass(MyFlowMapper.class);
        job.setReducerClass(MyFlowReducer.class);
        //4 设置mapper和reducer的输入输出格式
        job.setMapOutputKeyClass(MyFlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyFlowBean.class);
        //5 设置输入输出数据
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //设置分区
        job.setPartitionerClass(MyPartitioner2.class);
        job.setNumReduceTasks(5);


        //6 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
//        1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
//        2	13846544121	192.196.100.2			264	0	200
//        3 	13956435636	192.196.100.3			132	1512	200
//        4 	13966251146	192.168.100.1			240	0	404
//        5 	18271575951	192.168.100.2	www.atguigu.com	1527	2106	200
//        6 	84188413	192.168.100.3	www.atguigu.com	4116	1432	200
//        7 	13590439668	192.168.100.4			1116	954	200
//        8 	15910133277	192.168.100.5	www.hao123.com	3156	2936	200
//        9 	13729199489	192.168.100.6			240	0	200
//        10 	13630577991	192.168.100.7	www.shouhu.com	6960	690	200
//        11 	15043685818	192.168.100.8	www.baidu.com	3659	3538	200
//        12 	15959002129	192.168.100.9	www.atguigu.com	1938	180	500
//        13 	13560439638	192.168.100.10			918	4938	200
//        14 	13470253144	192.168.100.11			180	180	200
//        15 	13682846555	192.168.100.12	www.qq.com	1938	2910	200
//        16 	13992314666	192.168.100.13	www.gaga.com	3008	3720	200
//        17 	13509468723	192.168.100.14	www.qinghua.com	7335	110349	404
//        18 	18390173782	192.168.100.15	www.sogou.com	9531	2412	200
//        19 	13975057813	192.168.100.16	www.baidu.com	11058	48243	200
//        20 	13768778790	192.168.100.17			120	120	200
//        21 	13568436656	192.168.100.18	www.alibaba.com	2481	24681	200
//        22 	13568436656	192.168.100.19			1116	954	200