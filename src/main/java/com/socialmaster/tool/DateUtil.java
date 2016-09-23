package com.socialmaster.tool;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by liuxiaojun on 2016/8/26.
 */
public class DateUtil {
    /**
     * 计算时间差（单位：分钟）
     * @param startMinute  格式为：yyyyMMddHHmm
     * @param endMinute    格式为：yyyyMMddHHmm
     * @return long
     * @throws ParseException
     */
    public static long getMinuteDiff(String startMinute, String endMinute) {
        long minutes = -1;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
            long startTimeStamp = sdf.parse(startMinute).getTime();
            long endTimeStamp = sdf.parse(endMinute).getTime();
            minutes = (long)((endTimeStamp-startTimeStamp)/(1000*60));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return minutes;
    }

}
