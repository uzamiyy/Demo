package com.atguigu.test;
import com.atguigu.constants.Constants;
import com.atguigu.dao.HBaseDao;
import com.atguigu.utils.HBaseUtil;
import java.io.IOException;


public class Testweibo{
    public static void init(){
        try{
            HBaseUtil.createNameSpace(Constants.NAMESPACE);
            HBaseUtil.createtable(Constants.CONTENT_TABLE,Constants.content_table_version,Constants.CONTENT_TABLE_CF);

            HBaseUtil.createtable(Constants.relationshiptable,Constants.relationshiptable_version,
                    Constants.relationshiptable_cf,Constants.relationshiptable_cf2);

            HBaseUtil.createtable(Constants.inbox,Constants.inbox_version,Constants.inbox_cf);
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException{
        init();
        HBaseDao.publishweibo("1001","good day");
        HBaseDao.addAttends("1002","1001","1003");
        HBaseDao.getInit("1002");
        System.out.println("*******111*****");
        HBaseDao.publishweibo("1003","good day1");
        HBaseDao.getInit("1002");
        System.out.println("*******222*****");
        HBaseDao.deleteAttends("1002","1003");
        HBaseDao.getInit("1002");
        System.out.println("*******333*****");
        HBaseDao.addAttends("1002","1003");
        HBaseDao.getInit("1002");
        System.out.println("*******444*****");
        HBaseDao.getweibo("1001");
    }
}