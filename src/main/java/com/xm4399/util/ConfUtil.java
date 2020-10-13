package com.xm4399.util;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        InputStream in = new ConfUtil().getClass().getResourceAsStream("/config/application.properties");
        try {
            prop.load(in);
            return prop.getProperty(key);
        } catch (IOException e) {
            final Logger logger = LoggerFactory.getLogger(ConfUtil.class);
            logger.error("获取参数失败:", e);
            System.out.println(e.toString());
        }
        return null;
    }


}
