package com.pt.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DateUtil {
    private static ThreadLocal<Map<String, SimpleDateFormat>> dateFormatThreadLocal = ThreadLocal.withInitial(HashMap::new);

    public static String format(Date date, String pattern) {
        Map<String, SimpleDateFormat> dateFormatMap = dateFormatThreadLocal.get();
        SimpleDateFormat simpleDateFormat = dateFormatMap.get(pattern);
        if (simpleDateFormat == null) {
            synchronized (DateUtil.class) {
                if (!dateFormatMap.containsKey(pattern)) {
                    simpleDateFormat = new SimpleDateFormat(pattern);
                    dateFormatMap.put(pattern, simpleDateFormat);
                } else {
                    simpleDateFormat = dateFormatMap.get(pattern);
                }
            }
        }
        return simpleDateFormat.format(date);
    }

    public static Date parse(String str, String pattern) throws ParseException {
        Map<String, SimpleDateFormat> dateFormatMap = dateFormatThreadLocal.get();
        SimpleDateFormat simpleDateFormat = dateFormatMap.get(pattern);
        if (simpleDateFormat == null) {
            synchronized (DateUtil.class) {
                if (!dateFormatMap.containsKey(pattern)) {
                    simpleDateFormat = new SimpleDateFormat(pattern);
                    dateFormatMap.put(pattern, simpleDateFormat);
                } else {
                    simpleDateFormat = dateFormatMap.get(pattern);
                }
            }
        }
        return simpleDateFormat.parse(str);
    }
}
