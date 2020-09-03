package com.xm4399.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Auther: czk
 * @Date: 2020/9/3
 * @Description:
 */
public class ConfUtil {
    public  String getValue(String key){
        Properties prop = new Properties();
        InputStream in = new ConfUtil().getClass().getResourceAsStream("/config.properties");
        try {
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop.getProperty(key);
    }
}
