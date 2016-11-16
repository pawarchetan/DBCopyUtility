package com.dbcopy.factory;

import com.dbcopy.model.Table;
import com.dbcopy.util.Copier;
import com.dbcopy.util.Database;
import com.dbcopy.util.PooledCopier;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class CopierFactory {

    private DatabaseFactory databaseFactory;

    public CopierFactory(DatabaseFactory databaseFactory) {
        this.databaseFactory = databaseFactory;
    }

    public List<Copier> createPooledCopiers(String sourceDatabaseType, String sourceConnectionString,
                                            String targetDatabaseType, String targetConnectionString, int workerCount,
                                            Queue<Table> tablePool) {
        List<Copier> copiers = new ArrayList<>(workerCount);
        for (int i = 1; i <= workerCount; i++) {
            Database sourceDatabase = getDatabaseFactory().createDatabase(sourceDatabaseType, sourceConnectionString);
            Database targetDatabase = getDatabaseFactory().createDatabase(targetDatabaseType, targetConnectionString);
            copiers.add(new PooledCopier(sourceDatabase, targetDatabase, tablePool));
        }
        return copiers;
    }

    private DatabaseFactory getDatabaseFactory() {
        return this.databaseFactory;
    }

}
