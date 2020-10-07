package com.xm4399.test;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.time.ZoneId;
import java.util.stream.Stream;

import static org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo;

/**
 * @Auther: czk
 * @Date: 2020/9/15
 * @Description:
 */
public class RowDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableSchema physicalSchema = TableSchema.builder()
                .field("id", DataTypes.INT()).field("name",DataTypes.STRING()).field("sex",DataTypes.STRING()).build();//field("id",INT).field("name",STRING).
        RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        DataType internalDataType = DataTypeUtils.transform(physicalSchema.toRowDataType(), TypeTransformations.TO_INTERNAL_CLASS);
        TypeInformation<RowData> typeInfo = (TypeInformation<RowData>) fromDataTypeToTypeInfo(internalDataType);
        //inventoryDatabase.createAndInitialize();

        SourceFunction<RowData> sourceFunction = MySQLSource.<RowData>builder()
                .hostname("localhost")
                .port(3306)
                //.databaseList(inventoryDatabase.getDatabaseName()) // monitor all tables under inventory database
                .tableList("chenzhikun.kafka_format_test3")
                .username("root")
                .password("a5515458")
                .deserializer(new RowDataDebeziumDeserializeSchema(rowType, typeInfo, ((rowData, rowKind) -> {
                }),
                        ZoneId.of("UTC")))
                .build();


        env.addSource(sourceFunction).map(rowData -> {
            System.out.println(rowData.getRowKind() + " --- " + rowData.getArity() +  " --- " + rowData.getInt(0)+" --- "
                    +rowData.getString(1) + " --- " + rowData.getString(2) );
            System.out.println();
            return rowData;
        });

        env.execute("Print MySQL Snapshot + Binlog");
    }
}

