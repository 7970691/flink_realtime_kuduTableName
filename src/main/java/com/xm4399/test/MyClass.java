package com.xm4399.test;

import org.apache.flink.types.RowKind;
import org.apache.kafka.connect.data.Struct;

import java.io.Serializable;
import java.util.HashMap;


/**
 * @Auther: czk
 * @Date: 2020/9/17
 * @Description:
 */
public class MyClass  {
    public static void main(String[] args) {
        HashMap<String, String> n = new HashMap<>();
        n.put("1","a");
        n.put("2","b");
        for (String a : n.values()){
            System.out.println(a);
        }
    }
}
