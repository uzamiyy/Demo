package com.atguigu.constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class Constants {
    public static final Configuration CONFIGURATION = HBaseConfiguration.create();

    public static final String NAMESPACE =  "weibo";
    public static final String CONTENT_TABLE = "weibo:Content";
    public static final String CONTENT_TABLE_CF  = "info";
    public static final int content_table_version = 1;

    public static final String relationshiptable = "weibo:Relationships";
    public static final String relationshiptable_cf = "attends";
    public static final String relationshiptable_cf2 = "fans";
    public static final int relationshiptable_version = 1;

    public static final String inbox = "weibo:Inbox";
    public static final String inbox_cf = "info";
    public static final int inbox_version = 2;


}
