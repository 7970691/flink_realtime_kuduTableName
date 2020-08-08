package com.xm4399.util;

/**
 * @Auther: czk
 * @Date: 2020/8/5
 * @Description:
 */
public class ConfArgsPojo {

    private String address;
    private String username;
    private String password;
    private String dbName;
    private String tableName;
    private String isSubTable;
    private String isRealtime;

    public ConfArgsPojo() {
    }

    public ConfArgsPojo(String address, String username, String password, String dbName, String tableName, String isSubTable, String isRealtime) {
        this.address = address;
        this.username = username;
        this.password = password;
        this.dbName = dbName;
        this.tableName = tableName;
        this.isSubTable = isSubTable;
        this.isRealtime = isRealtime;
    }




    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getIsSubTable() {
        return isSubTable;
    }

    public void setIsSubTable(String isSubTable) {
        this.isSubTable = isSubTable;
    }

    public String getIsRealtime() {
        return isRealtime;
    }

    public void setIsRealtime(String isRealtime) {
        this.isRealtime = isRealtime;
    }
}
