package com.lornwolf.oracle.tool;

import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class RemoveModule {
    public static void main(String[] args) {
        Connection conn = null;

        String driver = "oracle.jdbc.driver.OracleDriver";
        String url = "jdbc:oracle:thin:@47.93.37.155:1521/IAP";
        String username = "IAP_DEMO";
        String password = "890-uiop";
        // String url = "jdbc:oracle:thin:@192.168.244.211:1521/KMCTEST2";
        // String username = "KMC_11001";
        // String password = "PWD11001";

        try {
        	FileOutputStream dropTableStream = new FileOutputStream("/drop_table.txt");
        	FileOutputStream deleteDataStream = new FileOutputStream("/delete_data.txt");

        	
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
            Statement stmt = conn.createStatement();

            DatabaseMetaData metaData = conn.getMetaData();
            /**
             * metaData.getTables(catalog, schemaPattern, tableNamePattern, types)
             * catalog 目录名称。
             * schema 架构名称模式（一般为用户名，注意大写），null查所有，不能用""代替null。
             * tableName 表名（null查所有）。
             * types 表类型的字符串数组。null查所有，不能用""代替null。
             **/
            ResultSet tables = metaData.getTables(null, username.toUpperCase(), null, new String[]{ "TABLE" } );
            while (tables.next()) {
                String resTableName = tables.getString("TABLE_NAME");
                if (resTableName.toUpperCase().contains("P17")) {
                    System.out.println(resTableName);
                    dropTableStream.write(("DROP TABLE " + resTableName + ";\r\n").getBytes());
                }

                ResultSet resultSet = stmt.executeQuery("select t.column_name from user_col_comments t where t.table_name = '" + resTableName + "'");
                while(resultSet.next()){
                    String fieldName = resultSet.getString("column_name");
                    if (fieldName.toUpperCase().contains("PARAMETER_CD")) {
                        System.out.println("    " + fieldName);
                        deleteDataStream.write(("DELETE FROM " + resTableName + " WHERE PARAMETER_CD LIKE %P17%;\r\n").getBytes());
                    }
                }
                resultSet.close();
            }
            stmt.close();
            tables.close();
            conn.close();

            dropTableStream.flush();
            dropTableStream.close();
            deleteDataStream.flush();
            deleteDataStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
