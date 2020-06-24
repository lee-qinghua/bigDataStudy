package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        String body = new String(event.getBody(), Charset.forName("UTF-8"));

        // body为原始数据，newBody为处理后的数据,判断是否为display的数据类型
        if (LogUtils.validateReportLog(body)) {
            return event;
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        ArrayList<Event> intercepts = new ArrayList<>();

        // 遍历所有Event，将拦截器校验不合格的过滤掉
        for (Event event : events) {

            Event interceptEvent = intercept(event);

            if (interceptEvent != null){
                intercepts.add(interceptEvent);
            }
        }

        return intercepts;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new LogETLInterceptor();
        }


        @Override
        public void configure(Context context) {

        }
    }
}