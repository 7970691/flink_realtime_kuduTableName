package com.xm4399.util;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.List;

/**
 * @Auther: czk
 * @Date: 2020/9/18
 * @Description:
 */
public class MyStringDebeziumDeserializeSchema implements DebeziumDeserializationSchema<MyStringClass> {
    private static final long serialVersionUID = -3928742139770041068L;

    @Override
    public void deserialize(SourceRecord record, Collector<MyStringClass> out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        MyStringClass myStringClass = new MyStringClass();
        Struct value = (Struct) record.value();
        Struct source = (Struct) value.getStruct("source");
        //获取表名
        String tableNmae = source.get("table").toString();
        myStringClass.setTableName(tableNmae);

        //是否快照
        String isSnapshot = source.getString("snapshot");
        if (isSnapshot == null) {
            isSnapshot = "false";
        }
        myStringClass.setSnapshot(isSnapshot);

        //获取主键
        Struct mysqlPk = (Struct) record.key();
        List<Field> pkList = mysqlPk.schema().fields();
        HashMap<String, String> mysqlPkMap = new HashMap<>();
        for (Field field : pkList) {
            String pkName = field.name();
            String pkValue = mysqlPk.get(pkName).toString();
            mysqlPkMap.put(pkName, pkValue);
        }
        myStringClass.setPrikey(mysqlPkMap);


        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            myStringClass.setRowKind("INSERT");
            setValues2MyStringClass( myStringClass, value, "after");
            out.collect(myStringClass);
        } else if (op == Envelope.Operation.DELETE) {
            myStringClass.setRowKind("DELETE");
            setValues2MyStringClass( myStringClass, value, "before");
            out.collect(myStringClass);
        } else {
            myStringClass.setRowKind("UPDATE");
            setValues2MyStringClass(myStringClass, value, "after");
            out.collect(myStringClass);
        }
    }

    //根据不同的rowKind决定获取after Struct还是before Struct
    public void setValues2MyStringClass( MyStringClass myStringClass, Struct value, String afterOrBefore) {
        List<Field> allFieldsList = value.getStruct(afterOrBefore).schema().fields();
        HashMap<String, String> mysqlAllFieldsMap = new HashMap<>();
        for (Field field : allFieldsList) {
            String fieldName = field.name();
            String fieldValue ;
            if (value.getStruct(afterOrBefore).get(fieldName) == null){
                fieldValue = null;
            }else {
                fieldValue = value.getStruct(afterOrBefore).get(fieldName).toString();
            }
            mysqlAllFieldsMap.put(fieldName, fieldValue);
        }
        myStringClass.setValues(mysqlAllFieldsMap);
    }

    @Override
    public TypeInformation<MyStringClass> getProducedType() {
        return TypeInformation.of(MyStringClass.class);
    }

}
