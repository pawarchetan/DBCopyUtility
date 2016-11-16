package com.dbcopy.startup;

import com.dbcopy.factory.CopierFactory;
import com.dbcopy.factory.DatabaseFactory;
import com.dbcopy.listener.ConsoleCopierListener;
import com.dbcopy.model.Table;
import com.dbcopy.util.Copier;
import com.dbcopy.util.CopierTask;
import com.dbcopy.util.Database;
import lombok.extern.log4j.Log4j2;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

@Log4j2
public class StartDBCopyUtility {
    private static final String SOURCE_TYPE = "source.type";
    private static final String TARGET_TYPE = "target.type";
    private static final String SOURCE_CONNECTION_STRING = "source.connection.string";
    private static final String TARGET_CONNECTION_STRING = "target.connections.string";
    private static final String EXCLUDE_TABLES = "exclude.tables";
    private static final String WORKER_THREAD = "worker.thread";
    private static final String PROPERTY_DEFAULT_VALUE = "";

    public static void main(String[] args) throws Exception {
        log.info("Getting properties from Config properties file");
        Properties properties = loadProperties();
        String sourceType = properties.getProperty(SOURCE_TYPE, PROPERTY_DEFAULT_VALUE);
        String sourceConnectionString = properties.getProperty(SOURCE_CONNECTION_STRING, PROPERTY_DEFAULT_VALUE);
        String targetType = properties.getProperty(TARGET_TYPE, PROPERTY_DEFAULT_VALUE);
        String targetConnectionString = properties.getProperty(TARGET_CONNECTION_STRING, PROPERTY_DEFAULT_VALUE);
        String excludes = properties.getProperty(EXCLUDE_TABLES, PROPERTY_DEFAULT_VALUE);
        int maxWorkers = new Integer(properties.getProperty(WORKER_THREAD, "1"));
        List<String> excludeTables = createFilterList(excludes);

        if (sourceType.length() == 0 || sourceConnectionString.length() == 0 || targetType.length() == 0
                || targetConnectionString.length() == 0 || maxWorkers == -1) {
            log.error("Error : source.connection.string or target.connections.string is empty...!!!!!");
            throw new Exception("source.connection.string or target.connections.string is empty...!!!!!");
        }
        try {
            DatabaseFactory databaseFactory = new DatabaseFactory();
            Database sourceDatabase = databaseFactory.createDatabase(sourceType, sourceConnectionString);

            sourceDatabase.connect();
            Queue<Table> pool = new LinkedBlockingQueue<>(sourceDatabase.getTables(excludeTables));
            final List<Thread> workers = new ArrayList<>(maxWorkers);
            ConsoleCopierListener console = new ConsoleCopierListener(true);

            CopierFactory copierFactory = new CopierFactory(databaseFactory);
            List<Copier> pooledCopiers = copierFactory.createPooledCopiers(sourceType, sourceConnectionString,
                    targetType, targetConnectionString, maxWorkers, pool);

            for (Copier copier : pooledCopiers) {
                copier.addCopierListener(console);
                workers.add(new Thread(new CopierTask(copier)));
            }
            workers.forEach(java.lang.Thread::start);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
    }

    private static Properties loadProperties() {
        //TODO path to properties file
        return loadProperties("TODO");
    }

    private static Properties loadProperties(String configFile) {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(configFile));
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return props;
    }

    public static List<String> createFilterList(String commaSeparatedFilters) {
        String[] raw = commaSeparatedFilters.split(",");
        List<String> filters = new ArrayList<>(raw.length);

        if (raw.length > 0 && raw[0].length() > 0) {
            for (String filter : raw) {
                filters.add(filter.trim());
            }
        }
        return filters;
    }
}
