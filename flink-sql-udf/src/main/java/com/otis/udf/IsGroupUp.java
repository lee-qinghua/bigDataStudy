package com.otis.udf;

import org.apache.flink.table.functions.ScalarFunction;

//判断是否成年
public class IsGroupUp extends ScalarFunction {
    public boolean eval(int age) {
        return age > 18;
    }
}
