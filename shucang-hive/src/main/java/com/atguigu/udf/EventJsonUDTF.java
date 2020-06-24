package com.atguigu.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;

public class EventJsonUDTF extends GenericUDTF {
    //该方法中，我们将指定输出参数的名称和参数类型：
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        // 获取传入的et
        String input = objects[0].toString();
        if (StringUtils.isBlank(input)) {
            return;
        } else {
            try {
                JSONArray jsonArray = new JSONArray(input);
                if (jsonArray == null)
                    return;
                // 循环遍历每一个事件
                for (int i = 0; i < jsonArray.length(); i++) {
                    String[] result = new String[2];
                    result[0] = jsonArray.getJSONObject(i).getString("en");
                    result[1] = jsonArray.getString(i);
                    forward(result);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
