package com.xm4399.tt;

import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;

/**
 * @Auther: czk
 * @Date: 2020/9/18
 * @Description:
 */
public class MyStringClass {
    String isSnapshot;
    private HashMap<String,String> prikey = new HashMap<>();
    private HashMap<String,String> values = new HashMap<>();
    private String rowKind;

    public MyStringClass(){ }

    public MyStringClass(String isSnapshot, HashMap<String, String> prikey, HashMap<String, String> values, String rowKind) {
        this.isSnapshot = isSnapshot;
        this.prikey = prikey;
        this.values = values;
        this.rowKind = rowKind;
    }

    public String getIsSnapshot() {
        return isSnapshot;
    }

    public void setSnapshot(String snapshot) {
        this.isSnapshot = snapshot;
    }

    public HashMap<String, String> getPrikey() {
        return prikey;
    }

    public void setPrikey(HashMap<String, String> prikey) {
        this.prikey = prikey;
    }

    public HashMap<String, String> getValues() {
        return values;
    }

    public void setValues(HashMap<String, String> values) {
        this.values = values;
    }

    public String getRowKind() {
        return rowKind;
    }

    public void setRowKind(String rowKind) {
        this.rowKind = rowKind;
    }

    @Override
    public String toString() {
        return "MyStringClass{" +
                "isSnapshot=" + isSnapshot +
                ", prikey=" + prikey.get("id")+
                ", values=" + values +
                ", rowKind='" + rowKind + '\'' +
                '}';
    }
}
