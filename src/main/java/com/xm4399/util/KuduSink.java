package com.xm4399.util;

import com.alibaba.fastjson.JSON;
import com.xm4399.test.MyClass;
import com.xm4399.tt.MyStringClass;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kudu.client.*;

import java.util.ArrayList;

public class KuduSink extends RichSinkFunction<MyStringClass>  {
    KuduUtil kuduUtil = null;

    private String tableName;
    private String isSubTable;
    private String kuduTableName;
    Long time = System.currentTimeMillis();

    public KuduSink(String tableName, String isSubTable, String kuduTableName) {
        this.tableName = tableName;
        this.isSubTable = isSubTable;
        this.kuduTableName = kuduTableName;
    }

    @Override
    public void invoke(MyStringClass value, Context context) throws Exception {
        if (null == kuduUtil) {
            kuduUtil = new KuduUtil();
        }
        processEveryRow(value, kuduUtil, tableName, isSubTable, kuduTableName);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        kuduUtil = new KuduUtil();
    }

    @Override
    public void close() throws Exception {
        super.close();
        kuduUtil.close();
    }

    public void processEveryRow( MyStringClass row, KuduUtil kuduUtil,  String tableName, String isSubTable, String kuduTableName) throws KuduException {
        Boolean isFlush = false; //是否需要flush
        ArrayList<String> tableNameList = new ArrayList<String>();
        KuduTable kuduTable = null;
        kuduTable = kuduUtil.getKuduTable(kuduTableName);
        String rowKind = row.getRowKind();
        if ("INSERT".equals(rowKind) || "UPDATE".equals(rowKind)) {
            if ((System.currentTimeMillis() - time) > 1000){  //500毫秒flush一次
                isFlush = true;
                time = System.currentTimeMillis();
            }
            kuduUtil.upsertRecordToKudu(kuduTable, row ,isFlush);
        } else if ("DELETE".equals(rowKind)) {
            kuduUtil.deleteRecordFromKudu(kuduTable, row);
        }
    }

}
