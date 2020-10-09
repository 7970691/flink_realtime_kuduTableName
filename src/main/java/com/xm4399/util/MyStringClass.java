package com.xm4399.util;

import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;

/**
 * @Auther: czk
 * @Date: 2020/9/18
 * @Description:
 */
public class MyStringClass {

    private String tableName;
    private String isSnapshot;
    private HashMap<String,String> prikey = new HashMap<>();
    private HashMap<String,String> values = new HashMap<>();
    private String rowKind; //INSERT,DELETE,UPDATE

    public MyStringClass(){ }
    public MyStringClass(String tableName, String isSnapshot, HashMap<String, String> prikey, HashMap<String, String> values, String rowKind) {
        this.tableName = tableName;
        this.isSnapshot = isSnapshot;
        this.prikey = prikey;
        this.values = values;
        this.rowKind = rowKind;
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
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
                "tableName='" + tableName + '\'' +
                ", isSnapshot='" + isSnapshot + '\'' +
                ", prikey=" + prikey +
                ", values=" + values +
                ", rowKind='" + rowKind + '\'' +
                '}';
    }
}
