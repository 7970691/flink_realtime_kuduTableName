package com.xm4399.tt;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import scala.annotation.meta.field;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * @Auther: czk
 * @Date: 2020/9/18
 * @Description:
 */
public class MyStringDebeziumDeserializeSchema implements DebeziumDeserializationSchema<MyStringClass>{
    private static final long serialVersionUID = -3928742139770041068L;

        @Override
        public void deserialize(SourceRecord record, Collector<MyStringClass> out) throws Exception {
            Envelope.Operation op = Envelope.operationFor(record);
            MyStringClass myStringClass = new MyStringClass();
            //是否快照
            Struct value =(Struct) record.value();
            Struct source =(Struct) value.getStruct("source");
            String isSnapshot = source.getString("snapshot");
            if (isSnapshot == null){
                isSnapshot = "false";
            }
            myStringClass.setSnapshot(isSnapshot);

            //获取主键
            Struct mysqlPk =(Struct) record.key();
            List<Field> pkList = mysqlPk.schema().fields();
            HashMap<String,String> mysqlPkMap = new HashMap<>();
            for (Field field : pkList){
                String pkName = field.name();
                String pkValue = mysqlPk.get(pkName).toString();
                mysqlPkMap.put(pkName, pkValue);
            }
            myStringClass.setPrikey(mysqlPkMap);

            //各字段键值对
            List<Field> allFieldsList = value.getStruct("after").schema().fields();
            HashMap<String,String> mysqlAllFieldsMap = new HashMap<>();
            for (Field field : allFieldsList){
                String fieldName = field.name();
                String fieldValue = value.getStruct("after").get(fieldName).toString();
                mysqlAllFieldsMap.put(fieldName, fieldValue);
            }
            myStringClass.setValues(mysqlAllFieldsMap);

            if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
                myStringClass.setRowKind("INSERT");
                out.collect(myStringClass);
            } else if (op == Envelope.Operation.DELETE) {
                myStringClass.setRowKind("DELETE");
                out.collect(myStringClass);
            } else {
                myStringClass.setRowKind("UPDATE");
                out.collect(myStringClass);
            }
        }

        @Override
        public TypeInformation<MyStringClass> getProducedType() {
            return TypeInformation.of(MyStringClass.class);
        }




    }
