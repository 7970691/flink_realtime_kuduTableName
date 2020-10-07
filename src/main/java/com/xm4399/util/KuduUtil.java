package com.xm4399.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xm4399.tt.MyStringClass;
import jdk.nashorn.internal.scripts.JO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kudu.*;
import org.apache.kudu.client.*;
import scala.annotation.meta.field;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;

public class KuduUtil implements Serializable {

    private static KuduClient kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051")
            .defaultAdminOperationTimeoutMs(60000).defaultOperationTimeoutMs(60000).build();

    private static  KuduSession session = getKuduSession();
    private static ColumnTypeAttributes decimalCol = new ColumnTypeAttributes.ColumnTypeAttributesBuilder().precision(15).scale(4).build();
    public KuduClient getKuduClient(){
        if(kuduClient != null){
            return kuduClient;
        }else{
            KuduClient kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051")
                    .defaultAdminOperationTimeoutMs(60000).defaultOperationTimeoutMs(60000).build();
        return kuduClient;
        }
    }

    public static KuduSession getKuduSession(){
         KuduSession session = kuduClient.newSession();
        //session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        session.setTimeoutMillis(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS);
       // session.setIgnoreAllDuplicateRows(true);
        session.setMutationBufferSpace(50000);
        session.setFlushInterval(500);
        return session;
    }

    public  void deleteRecordFromKudu(KuduTable kuduTable, MyStringClass record, String jobID) throws KuduException {
        Delete delete = kuduTable.newDelete();
        PartialRow row = delete.getRow();
        String pk = "";  //kudu主键对应的内容
        HashMap<String, String> allFieldsAndValues = record.getValues();
        Schema colSchema =kuduTable.getSchema();
        List <ColumnSchema> pkList = colSchema.getPrimaryKeyColumns();
        for(ColumnSchema item : pkList){
            String colName = item.getName();
            if("table_id" .equals(colName)){
                String subTableName = record.getTableName();
                String table_id = subTableName.substring(subTableName.lastIndexOf("_") + 1, subTableName.length());
                pk = table_id;  //kudu主键对应的内容
            }else {
                pk = allFieldsAndValues.getOrDefault(colName,"");
            }
            //int colIdx = schema.getColumnIndex(name);
            int colIdx = colSchema.getColumnIndex(colName);
            Type colType = item.getType();
            Common.DataType dataType = colType.getDataType(decimalCol);
            if(!"".equals(pk)){
                addRow(row,pk,colName,colIdx,colType,dataType);
            }
        }
        if (session == null){
            sessionClosedCancelJob(jobID);
        }
        session.apply(delete);
        session.flush();
    }

    public void upsertRecordToKudu(KuduTable kuduTable, MyStringClass record, String jobID) throws KuduException {
        HashMap<String, String> allFieldsAndValues = record.getValues();
        Upsert upsert = kuduTable.newUpsert();
        PartialRow row = upsert.getRow();
        Schema colSchema =kuduTable.getSchema();
        List<ColumnSchema> colList = colSchema.getColumns();
        for(ColumnSchema item : colList){
            String colName = item.getName();
            int colIdx = colSchema.getColumnIndex(colName);
            Type colType = item.getType();
            Common.DataType dataType = colType.getDataType(decimalCol);
            if(allFieldsAndValues.containsKey(colName)){
                try{
                    String field = allFieldsAndValues.get(colName);
                    //kudu单元格最大不超过64k,当内容超过16384位,将其截断
                    if(field.length() >= 16384){
                        field = field.substring(0,16380);
                    }
                    addRow(row,field,colName,colIdx,colType,dataType);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }else if("table_id" .equals(colName)){
                String subTableName = record.getTableName();
                String table_id = subTableName.substring(subTableName.lastIndexOf("_") + 1, subTableName.length());
                row.addShort(colIdx, Short.parseShort(table_id));
                addRow(row,table_id,colName,colIdx,colType,dataType);
            }
        }
        if (session == null){
           sessionClosedCancelJob(jobID);
        }
        session.apply(upsert);
        if ("false".equals(record.getIsSnapshot()) || "last".equals(record.getIsSnapshot())){
            double d = Math.random();
            System.out.println("before" + System.currentTimeMillis()+ "-------" + d);
            try {
                session.flush();
            } catch(Exception e){
                e.printStackTrace();
            }
            System.out.println("after--" + System.currentTimeMillis()+ "-------" + d);
        }
    }
   /* private int getColumnIndex(Schema columns, String colName){
        try{
            System.out.println(columns.getColumnIndex(colName));
            return columns.getColumnIndex(colName);
            //System.out.println(columns.getColumnIndex(colName));
        }catch(Exception e){
            e.printStackTrace();
        }finally {

            return -1;
        }
    }*/

    public KuduTable getKuduTable(String tableName) throws KuduException {
        KuduTable kuduTable = null;
        if (kuduClient == null){
            kuduClient = getKuduClient();
        }
        kuduTable = kuduClient.openTable(tableName);
        return kuduTable;
    }

    // session 为null时,cancel并根据checkpoint重启flink job
    public void sessionClosedCancelJob(String jobID){
        System.out.println("session is null >>>>>>>>>>>>>>>>>>>>>>");
        new JDBCUtil().insertErroeInfo(jobID , "", "kudu session is null, maybe closed");
        FlinkRestApiUtil flinkRestApiUtil = new FlinkRestApiUtil();
        String flinkJobID = flinkRestApiUtil.getFlinkJobOneInfo("flink-cdc-" + jobID, "jid");
        flinkRestApiUtil.cancalFlinkJob(flinkJobID);
        String checkpointPath = flinkRestApiUtil.getCheckPointPath(flinkJobID);
        new JDBCUtil().insertCheckPointInfo(jobID, flinkJobID, checkpointPath);
    }

    private void addRow(PartialRow row, String field, String colName, int colIdx, Type colType, Common.DataType dataType ){
        switch(dataType){
            case BOOL :
                row.addBoolean(colIdx, Boolean.parseBoolean(field));
                break;
            case FLOAT :
                row.addFloat(colIdx, Float.parseFloat(field));
                break;
            case DOUBLE :
                row.addDouble(colIdx, Double.parseDouble(field));
                break;
            case BINARY :
                row.addBinary(colIdx, field.getBytes());
                break;
            case INT8 :
                row.addByte(colIdx, Byte.parseByte(field));
                break;
            case INT16 :
                //                            val temp = row.getShort(colName).toShort
                row.addShort(colIdx, Short.parseShort(field));
                break;
            case INT32 :
                row.addInt(colIdx, Integer.parseInt(field));
                break;
            case INT64 :
                row.addLong(colIdx, Long.parseLong(field));
                break;
            case STRING :
                row.addString(colIdx, field) ;
                break;
            case DECIMAL64 :
                row.addDecimal(colIdx,new BigDecimal(field,new MathContext(15,RoundingMode.HALF_UP)).setScale(4,RoundingMode.HALF_UP));

                break;
            default:
                throw new IllegalArgumentException("The provided data type doesn't map to know any known one.");
        }
    }

    public void close() throws KuduException {
        if (session != null){
            session.close();
        }
        if (kuduClient != null){
            kuduClient.close();
        }
    }

}
