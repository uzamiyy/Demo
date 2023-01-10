package com.atguigu.utils;
import com.atguigu.constants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import java.io.IOException;

public class HBaseUtil {
    public static void createNameSpace(String namespace) throws IOException{
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
        connection.close();

        System.out.println("创建命名空间成功");

    }
    public static boolean istableexist(String tablename) throws IOException{
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
        admin.tableExists(TableName.valueOf(tablename));
        admin.close();
        connection.close();
        return false;

    }
    public static void createtable(String tablename, int version,String... cfs) throws IOException{
        if(cfs.length<=0){
            System.out.println("plz set column infomation");
            return;
        }
        if(istableexist(tablename)){
            System.out.println(tablename+"table already exists");
            return;
        }
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tablename));
        for(String cf:cfs){
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
            columnDescriptor.setMaxVersions(version);
            tableDescriptor.addFamily(columnDescriptor);
        }
        admin.createTable(tableDescriptor);
        admin.close();
        connection.close();

    }
}
