package com.xm4399.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @Auther: czk
 * @Date: 2020/8/12
 * @Description:
 */
public class DateUtil {


    public static String getTimestampOfBeforeOneHourString() {
        // 获取Calendar对象并以传进来的时间为准
        Calendar calendar = Calendar.getInstance();
        Date date = new Date();
        calendar.setTime(date);
        // 将现在的时间滚动固定时长,转换为Date类型赋值
        calendar.add(Calendar.HOUR,-1);
        // 转换为Date类型再赋值
        date = calendar.getTime();
        System.out.println(date.getTime());
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = formatter.format(date);

        return dateString;
    }
    public static void main(String[] args) {
        //System.out.println(getBeforeOneHourString());
        Long startTs = System.currentTimeMillis();
        System.out.println(startTs);
        System.out.println(System.currentTimeMillis()- 3600000);
        getTimestampOfBeforeOneHourString();
        startTs = startTs - 3600000;
    }
    

}
