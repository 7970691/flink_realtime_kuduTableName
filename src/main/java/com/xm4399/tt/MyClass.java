package com.xm4399.tt;

import org.apache.flink.types.RowKind;
import org.apache.kafka.connect.data.Struct;


/**
 * @Auther: czk
 * @Date: 2020/9/17
 * @Description:
 */
public class MyClass {
    private Struct key;
    private Struct value;
    private Struct after;
    private RowKind rowKind;

    public MyClass() { }

    public MyClass(Struct key, Struct value, Struct after) {
        this.key = key;
        this.value = value;
        this.after = after;
    }

    public MyClass(Struct key, Struct value) {
        this.key = key;
        this.value = value;
    }

    public Struct getAfter() {
        return after;
    }

    public void setAfter(Struct after) {
        this.after = after;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Struct key) {
        this.key = key;
    }

    public Struct getValue() {
        return value;
    }

    public void setValue(Struct value) {
        this.value = value;
    }

    public RowKind getRowKind() {
        return rowKind;
    }

    public void setRowKind(RowKind rowKind) {
        this.rowKind = rowKind;
    }
}
