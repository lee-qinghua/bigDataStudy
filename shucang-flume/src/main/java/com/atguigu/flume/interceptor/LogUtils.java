package com.atguigu.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtils {

    private static Logger logger = LoggerFactory.getLogger(LogUtils.class);

    /**
     * 日志检查，正常的log会返回true，错误的log返回false
     *
     * @param log
     */
    public static boolean validateReportLog(String log) {

        try {
//          日志的格式是：时间戳| json串
//          1549696569054 | {"cm":{"ln":"-89.2","sv":"V2.0.4","os":"8.2.0","g":"M67B4QYU@gmail.com","nw":"4G","l":"en","vc":"18","hw":"1080*1920","ar":"MX","uid":"u8678","t":"1549679122062","la":"-27.4","md":"sumsung-12","vn":"1.1.3","ba":"Sumsung","sr":"Y"},"ap":"weather","et":[]}
            String[] logArray = log.split("\\|");

            if (logArray.length < 2) {
                return false;
            }
//          检查第一串是否为时间戳 或者不是全数字
            if (logArray[0].length() != 13 || !NumberUtils.isDigits(logArray[0])) {
                return false;
            }

//          第二串是否为正确的json,这里我们就粗略的检查了，有时候我们需要从后面来发现json传错的数据，做分析
            if (!logArray[1].trim().startsWith("{") || !logArray[1].trim().endsWith("}")) {
                return false;
            }
        } catch (Exception e) {
//          错误日志打印，需要查看
            logger.error("parse error,message is:" + log);
            logger.error(e.getMessage());

            return false;
        }

        return true;
    }
}