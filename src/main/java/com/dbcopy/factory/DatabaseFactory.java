package com.dbcopy.factory;

import com.dbcopy.util.Database;
import lombok.extern.log4j.Log4j2;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

@Log4j2
public class DatabaseFactory {

    public Database createDatabase(String databaseType, String connectionString) {
        Database database = null;

        try {
            Class<Database> databaseClass = (Class<Database>) Class.forName(databaseType);
            Constructor<?> ctor = databaseClass.getConstructor(String.class);
            database = (Database) ctor.newInstance(connectionString);
        } catch (ClassNotFoundException | SecurityException | NoSuchMethodException | IllegalArgumentException
                | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return database;
    }

}
