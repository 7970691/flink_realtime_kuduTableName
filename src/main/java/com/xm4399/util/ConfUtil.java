package com.xm4399.util;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;

/**
 * @Auther: czk
 * @Date: 2020/9/3
 * @Description:
 */
public class ConfUtil {
    /*public  String getValue(String key){
        Properties prop = new Properties();
        InputStream in = new ConfUtil().getClass().getResourceAsStream("/config.properties");
        try {
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop.getProperty(key);
    }*/
    // 获取apollo配置
    public String getValue(String key){
        Config config = ConfigService.getAppConfig(); //config instance is singleton for each namespace and is never null
        String someDefaultValue = "someDefaultValueForTheKey";
        String value = config.getProperty(key, someDefaultValue);
        return value;
    }
}
