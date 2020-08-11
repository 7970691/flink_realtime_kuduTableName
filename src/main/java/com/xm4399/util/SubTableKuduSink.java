package com.xm4399.util;

import com.alibaba.fastjson.JSON;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kudu.client.*;

import java.util.ArrayList;

public class SubTableKuduSink extends RichSinkFunction<ConsumerRecord<String,String>> {
    KuduUtil kuduUtil = null;
    KuduSession kuduSession = null;
    private  String address;
    private  String username;
    private  String password;
    private  String dbName;
    private  String tableName;
    private  String isSubTable;
    private  String isRealtime;
    private  String kuduTableName;


    public SubTableKuduSink(String address, String username, String password, String dbName, String tableName,
                            String isSubTable, String isRealtime, String kuduTableName){
        this.address = address;
        this.username = username;
        this.password = password;
        this.dbName = dbName;
        this.tableName =tableName;
        this.isSubTable = isSubTable;
        this.isRealtime = isRealtime;
        this.kuduTableName = kuduTableName;

    }

    @Override
    public void invoke(ConsumerRecord<String, String> value, Context context) throws Exception {
        if (null == kuduUtil){
            kuduUtil = new KuduUtil();
        }
        //String isSubTable = "true";
        processEveryRow(value,kuduUtil,address, username, password, dbName, tableName, isSubTable, isRealtime, kuduTableName );
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

    public KuduSession getKuduSession(KuduClient kuduClient){
        kuduSession = kuduClient.newSession();
        kuduSession.setTimeoutMillis(60000);
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kuduSession.setMutationBufferSpace(10000);
        return kuduSession;
    }

    //对每条数据进行处理
    public void processEveryRow(ConsumerRecord<String,String> row , KuduUtil kuduUtil, String address, String username, String password,
                                String dbName, String tableName, String isSubTable, String isRealtime, String kuduTableName) throws KuduException {
        //dbName = JSON.parseObject(row.value()).getOrDefault("database","").toString();
        String RowTableName = getTableName(row);
        //kuduTable = kuduClient.openTable(tableName);
        ArrayList<String> tableNameList = new ArrayList<String>();
        KuduTable kuduTable = null;
        if ("true".equals(isSubTable)){
            // String sumTableName = tableName.substring(0,tableName.lastIndexOf("_"));
            tableNameList = new ListAllSubTableName().listAllSmallTableName(address, username, password, dbName,tableName);
             kuduTable = kuduUtil.getKuduTable(kuduTableName);
        }else{
            tableNameList.add(tableName);
            kuduTable = kuduUtil.getKuduTable(kuduTableName);
        }
        if(tableNameList.contains(RowTableName)){
            String data = JSON.parseObject(row.value()).getOrDefault("data","").toString();
            if ("".equals(data)){
                System.out.println(tableName + "'s data is null.");
            }else{
                String rowType = JSON.parseObject(row.value()).getOrDefault("type","").toString();
                if("INSERT".equals(rowType) || "UPDATE".equals(rowType)){
                    kuduUtil.upsertRecordToKudu(kuduTable,row);
                    System.out.println("添加或更新了>>>>" +  data);
                }else if("DELETE".equals(rowType)){
                    kuduUtil.deleteRecordFromKudu(kuduTable,row);
                    System.out.println("删除了>>>>>  " +data);
                }
            }
        }else{
            System.out.println("表" + RowTableName + "不在kudu过滤范围");
        }
    }

    //获取表名
    public String getTableName(ConsumerRecord<String,String> row){
        String dbName = JSON.parseObject(row.value()).getOrDefault("database","").toString();
        String tableName = JSON.parseObject(row.value()).getOrDefault("table","").toString();
        return tableName;
    }
}
