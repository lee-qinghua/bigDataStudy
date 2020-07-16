package com.otis;

import org.apache.flink.connector.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.connector.jdbc.catalog.PostgresCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.tools.nsc.transform.patmat.ScalaLogic;

import java.util.ArrayList;
import java.util.List;

public class MyCatalog extends AbstractJdbcCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(MyCatalog.class);
    public static final String DEFAULT_DATABASE = "mysql";

    public MyCatalog(String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        ArrayList<String> list = new ArrayList<>();

        return null;
    }

    @Override
    public CatalogDatabase getDatabase(String s) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        ArrayList<String> list = new ArrayList<>();

        //查询当前的数据库是否存在
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return null;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return super.databaseExists(databaseName);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath objectPath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean tableExists(ObjectPath objectPath) throws CatalogException {
        return false;
    }
}
