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
        // String url = "jdbc:oracle:thin:@47.93.37.155:1521/IAP";
        // String username = "IAP_DEMO";
        // String password = "890-uiop";
        String url = "jdbc:oracle:thin:@192.168.244.211:1521/KMCTEST2";
        String username = "KMC_11001";
        String password = "PWD11001";

        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
            Statement stmt = conn.createStatement();

            FileOutputStream dropTableStream = new FileOutputStream("D:/drop_table.sql");
            FileOutputStream dropViewStream = new FileOutputStream("D:/drop_view.sql");

            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tables = metaData.getTables(null, username.toUpperCase(), null, new String[]{"TABLE", "VIEW"});

            while (tables.next()) {
                String tableType = tables.getString("TABLE_TYPE");
                String tableName = tables.getString("TABLE_NAME");

                // 出力删除表或数据的SQL。
                if (tableType.toUpperCase().equals("TABLE")) {
                    if (tableName.toUpperCase().contains("P17")) {
                        System.out.println("TABLE : " + tableName);
                        dropTableStream.write(("DROP TABLE " + username + "." + tableName + ";\r\n").getBytes());
                    } else {
                        ResultSet resultSet = stmt.executeQuery("select t.column_name from user_col_comments t where t.table_name = '" + tableName + "'");
                        FileOutputStream deleteDataStream = new FileOutputStream("D:/delete_data_" + tableName.toUpperCase() + ".sql");
                        while(resultSet.next()) {
                            String fieldName = resultSet.getString("column_name");
                            deleteDataStream.write(("DELETE FROM " + username + "." + tableName + " WHERE regexp_like(" + fieldName + ",'p17','i');\r\n").getBytes());
                        }
                        resultSet.close();
                        deleteDataStream.flush();
                        deleteDataStream.close();
                    }
                }

                // 出力删除视图的SQL。
                if (tableType.toUpperCase().equals("VIEW")) {
                    if (tableName.toUpperCase().contains("P17")) {
                        System.out.println("VIEW : " + tableName);
                        dropViewStream.write(("DROP VIEW " + username + "." + tableName + ";\r\n").getBytes());
                    }
                }
            }
            tables.close();
            dropTableStream.flush();
            dropTableStream.close();
            dropViewStream.flush();
            dropViewStream.close();

            // 出力删除代码的SQL。
            FileOutputStream dropSourceStream = new FileOutputStream("D:/drop_source.sql");
            ResultSet resultSet = stmt.executeQuery("SELECT DISTINCT OWNER, NAME, TYPE FROM ALL_SOURCE WHERE OWNER = '" + username + "'");
            while(resultSet.next()) {
                String sourceName = resultSet.getString("NAME");
                if (sourceName.toUpperCase().contains("P17")) {
                    dropSourceStream.write(("DROP " + resultSet.getString("TYPE") + " " + username + "." + sourceName + ";\r\n").getBytes());
                }
            }
            resultSet.close();
            dropSourceStream.flush();
            dropSourceStream.close();

            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
