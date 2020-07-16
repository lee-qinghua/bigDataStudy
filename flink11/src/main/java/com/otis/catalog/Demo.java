package com.otis;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.util.List;

public class Demo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name = "mypg";
        String defaultDatabase = "order1";
        String username = "root";
        String password = "root";
        String baseUrl = "jdbc:mysql://localhost:3306/";

        JdbcCatalog catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl);
        tableEnv.registerCatalog("mypg", catalog);

        // set the JdbcCatalog as the current catalog of the session
        tableEnv.useCatalog("mypg");

        //查看所有的catalog
//        String[] strings = tableEnv.listCatalogs();
//        for (String string : strings) {
//            System.out.println(string);
//        }

        List<String> order1 = catalog.listTables("order1");
        for (String s : order1) {
            System.out.println(s);
        }

//        List<String> strings = catalog.listDatabases();


        String baseUrl1 = catalog.getBaseUrl();
        System.out.println(baseUrl);

    }
}
