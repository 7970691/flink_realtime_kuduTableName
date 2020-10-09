package com.xm4399.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.client.*;

import java.util.ArrayList;

public class KuduSink extends RichSinkFunction<MyStringClass>  {
    KuduUtil kuduUtil = null;

    private String tableName;
    private String isSubTable;
    private String kuduTableName;
    private String jobID;

    public KuduSink(String tableName, String isSubTable, String kuduTableName, String jobID) {
        this.tableName = tableName;
        this.isSubTable = isSubTable;
        this.kuduTableName = kuduTableName;
        this.jobID = jobID;
    }

    @Override
    public void invoke(MyStringClass record, Context context) throws Exception {
        if (null == kuduUtil) {
            kuduUtil = new KuduUtil();
        }
        processEveryRow(record, kuduUtil, tableName, isSubTable, kuduTableName, jobID);
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

    public void processEveryRow( MyStringClass row, KuduUtil kuduUtil,  String tableName, String isSubTable, String kuduTableName, String jobID) throws KuduException {
        ArrayList<String> tableNameList = new ArrayList<String>();
        KuduTable kuduTable = null;
        kuduTable = kuduUtil.getKuduTable(kuduTableName);
        String rowKind = row.getRowKind();
        if ("INSERT".equals(rowKind) || "UPDATE".equals(rowKind)) {
            kuduUtil.upsertRecordToKudu(kuduTable, row , jobID);
        } else if ("DELETE".equals(rowKind)) {
            kuduUtil.deleteRecordFromKudu(kuduTable, row, jobID);
        }
    }

}
