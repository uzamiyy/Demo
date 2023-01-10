package com.atguigu.dao;

import com.atguigu.constants.Constants;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;

import java.io.IOException;
import java.util.ArrayList;
public class HBaseDao {
    public static void publishweibo(String uid,String content) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table table = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        long ts = System.currentTimeMillis();
        String rowKey = uid + "_" + ts;
        Put contPut = new Put(Bytes.toBytes(rowKey));
        contPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes("content"),Bytes.toBytes(content));
        table.put(contPut);
        Table relaTable = connection.getTable(TableName.valueOf(Constants.relationshiptable));
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.relationshiptable_cf2));
        Result result = relaTable.get(get);

        ArrayList<Put> inboxPuts = new ArrayList<>();

        for (Cell cell : result.rawCells()) {
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            inboxPut.addColumn(Bytes.toBytes(Constants.inbox_cf), Bytes.toBytes(uid),Bytes.toBytes(rowKey));
            inboxPuts.add(inboxPut);

        }
        if(inboxPuts.size()>0){

            Table inboxTable=connection.getTable(TableName.valueOf(Constants.inbox));
            inboxTable.put(inboxPuts);
            inboxTable.close();

        }
        relaTable.close();
        table.close();
        connection.close();
    }
    public static void addAttends(String uid, String... attends) throws IOException{
        if (attends == null || attends.length<=0||uid==null||uid.length()<=0){
            System.out.println("plz select the user to follow");
            return;
        }

        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table relatable = connection.getTable(TableName.valueOf(Constants.relationshiptable));
        ArrayList<Put> relaPuts = new ArrayList<>();
        Put uidPut = new Put(Bytes.toBytes(uid));
        for(String attend: attends){
            uidPut.addColumn(Bytes.toBytes(Constants.relationshiptable_cf), Bytes.toBytes(attend),Bytes.toBytes(attend));
            relaPuts.add(uidPut);
            Put attendPut = new Put(Bytes.toBytes(attend));
            attendPut.addColumn(Bytes.toBytes(Constants.relationshiptable_cf2), Bytes.toBytes(uid),Bytes.toBytes(uid));
            relaPuts.add(attendPut);
        }
        relaPuts.add(uidPut);
        relatable.put(relaPuts);
        Table conTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Put inboxPut = new Put(Bytes.toBytes(uid));
        for (String attend:attends){
            Scan scan = new Scan(Bytes.toBytes(attend+'_'),Bytes.toBytes(attend+'|'));
            ResultScanner resultScanner = conTable.getScanner(scan);
            long ts = System.currentTimeMillis();
            for (Result result:resultScanner){
                inboxPut.addColumn(Bytes.toBytes(Constants.inbox_cf), Bytes.toBytes(attend), ts++, result.getRow());

            }
        }
        if(!inboxPut.isEmpty()){
            Table inboxTable=connection.getTable(TableName.valueOf(Constants.inbox));
            inboxTable.put(inboxPut);
            inboxTable.close();
        }
        relatable.close();
        conTable.close();
        connection.close();
    }
    public static void deleteAttends(String uid, String... dels) throws IOException{
        if(dels.length<=0){
            System.out.println("plz add user are pending");
            return;
        }
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table relatable = connection.getTable(TableName.valueOf(Constants.relationshiptable));
        ArrayList<Delete> relaDeletes = new ArrayList<>();
        Delete uidDelete = new Delete(Bytes.toBytes(uid));
        for(String del: dels){
            uidDelete.addColumns(Bytes.toBytes(Constants.relationshiptable_cf), Bytes.toBytes(del));
            Delete delDelete =new Delete(Bytes.toBytes(del));
            delDelete.addColumns(Bytes.toBytes(Constants.relationshiptable_cf2),Bytes.toBytes(uid));
            relaDeletes.add(delDelete);
        }
        relaDeletes.add(uidDelete);
        relatable.delete(relaDeletes);
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.inbox));
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));
        for (String del:dels){
            inboxDelete.addColumns(Bytes.toBytes(Constants.inbox_cf), Bytes.toBytes(del));
        }
        inboxTable.delete(inboxDelete);
        relatable.close();
        inboxTable.close();
        connection.close();
    }
    public static void getInit(String uid) throws IOException{
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.inbox));
        Table conTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);
        for(Cell cell : result.rawCells()){
            Get contGet = new Get(CellUtil.cloneValue(cell));
            Result conResult = conTable.get(contGet);
            for(Cell conCell : conResult.rawCells()){
                System.out.println("RK"+Bytes.toString(CellUtil.cloneRow(conCell))+
                        ",CF:"+Bytes.toString(CellUtil.cloneFamily(conCell))+
                        ",CN"+Bytes.toString(CellUtil.cloneQualifier(conCell))+
                        ",value"+Bytes.toString(CellUtil.cloneValue(conCell)));

            }
            inboxTable.close();
            conTable.close();
            connection.close();
        }
    }
    public static void getweibo(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table table = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Scan scan = new Scan();
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(uid+"_"));
        scan.setFilter(rowFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RK"+Bytes.toString(CellUtil.cloneRow(cell))+
                        ",CF:"+Bytes.toString(CellUtil.cloneFamily(cell))+
                        ",CN"+Bytes.toString(CellUtil.cloneQualifier(cell))+
                        ",value"+Bytes.toString(CellUtil.cloneValue(cell)));

            }
        }
        table.close();
        connection.close();
    }






}
