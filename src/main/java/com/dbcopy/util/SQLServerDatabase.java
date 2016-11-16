package com.dbcopy.util;

import com.dbcopy.model.Field;
import com.dbcopy.model.Table;
import lombok.extern.log4j.Log4j2;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Log4j2
public class SQLServerDatabase implements Database {
    private static final String SQL_SERVER_CLASS_NAME = "net.sourceforge.jtds.jdbc.Driver";
    private String connectionString;
    private Connection connection = null;

    public SQLServerDatabase(String connectionString) {
        this.connectionString = connectionString;
    }

    public void connect() throws Exception {
        if (getConnection() == null) {
            Connection connection;

            Class.forName(SQL_SERVER_CLASS_NAME);
            connection = DriverManager.getConnection(connectionString);
            connection.setAutoCommit(true);

            setConnection(connection);
        }
    }

    @Override
    public List<Table> getTables(List<String> excludes) {
        String filter = prepareIncludeAndExcludeClause(excludes);
        List<Table> tables = new ArrayList<>();
        String query = "SELECT TABLE_CATALOG, TABLE_NAME, TABLE_SCHEMA FROM information_schema.tables WHERE TABLE_TYPE=?" + filter;
        String tableType = "BASE TABLE";

        try {
            PreparedStatement statement = getConnection().prepareStatement(query);
            statement.setString(1, tableType);
            ResultSet result = statement.executeQuery();

            while (result.next()) {
                String catalog = result.getString("TABLE_CATALOG");
                String schema = result.getString("TABLE_SCHEMA");
                String name = result.getString("TABLE_NAME");

                Table table = new Table(catalog, schema, name);
                table.setFields(getFieldsForTable(table)); // important :)
                tables.add(table);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return tables;
    }


    @Override
    public int countContentsForTable(Table table) {
        int count = 0;
        String query = "SELECT count(1) FROM " + buildTableName(table);
        try {
            Statement statement = getConnection().createStatement();
            ResultSet result = statement.executeQuery(query);
            result.next();
            count = result.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return count;
    }

    @Override
    public ResultSet getContentsForTable(Table table) throws SQLException {
        String query = "SELECT * FROM " + buildTableName(table);
        Statement statement = getConnection().createStatement();
        return statement.executeQuery(query);
    }

    @Override
    public String buildTableName(Table table) {
        return "[" + table.getCatalog() + "].[" + table.getSchema() + "].[" + table.getName() + "]";
    }

    @Override
    public PreparedStatement buildPreparedInsertStatement(Table sourceTable, Table targetTable) throws Exception {
        List<Field> fields = sourceTable.getFields();
        StringBuilder query = new StringBuilder("INSERT INTO ");
        StringBuilder values = new StringBuilder("VALUES(");

        query.append(buildTableName(targetTable));
        query.append(" (");

        for (int i = 0, l = fields.size(); i < l; i++) {
            Field field = fields.get(i);

            query.append("[");
            query.append(field.getName());
            query.append("]");

            values.append("?");

            if (i < l - 1) {
                query.append(",");
                values.append(",");
            }
        }

        query.append(") ");
        values.append(")");
        query.append(values);

        return createPreparedStatement(query.toString());
    }

    public Connection getConnection() {
        return this.connection;
    }

    private void setConnection(Connection connection) {
        this.connection = connection;
    }

    private String prepareIncludeAndExcludeClause(List<String> excludes) {
        String filter;
        filter = prepareFilterClause(" AND TABLE_NAME NOT IN(", excludes);
        return filter;
    }

    private String prepareFilterClause(String clause, List<String> excludes) {
        String filter = "";

        if (excludes.size() > 0) {
            StringBuilder buffer = new StringBuilder(clause);
            for (int i = 0, l = excludes.size(); i < l; i++) {
                if (i > 0) buffer.append(",");
                buffer.append("'");
                buffer.append(excludes.get(i));
                buffer.append("'");
            }
            buffer.append(")");
            filter = buffer.toString();
        }
        return filter;
    }

    private List<Field> getFieldsForTable(Table table) {
        List<Field> fields = new ArrayList<>();

        try {
            DatabaseMetaData meta = getConnection().getMetaData();
            ResultSet columns = meta.getColumns(null, null, table.getName(), null);

            while (columns.next()) {
                String column;
                if ("DATETIME".equals(columns.getString(6).toUpperCase()) || "DATE".equals(columns.getString(6).toUpperCase())
                        || "BIT".equals(columns.getString(6).toUpperCase()) || "INT".equals(columns.getString(6).toUpperCase())
                        || "SMALLINT".equals(columns.getString(6).toUpperCase()) || "FLOAT".equals(columns.getString(6).toUpperCase())) {
                    column = columns.getString(6).toUpperCase();
                } else {
                    if (columns.getString(7).equals("2147483647")) {
                        column = columns.getString(6).toUpperCase() + "(MAX)";
                    } else {
                        column = columns.getString(6).toUpperCase() + "(" + columns.getString(7) + ")";
                    }
                }
                Field field = new Field(columns.getString("COLUMN_NAME"), column, columns.getInt("DATA_TYPE"));
                fields.add(field);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }

        return fields;
    }

    private boolean enableConstraints(boolean enabled) {
        boolean result = true;
        String query = "EXEC sp_msforeachtable \"ALTER TABLE ? NOCHECK CONSTRAINT all\"";
        if (enabled) query = "EXEC sp_msforeachtable \"ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all\"";

        try {
            Statement statement = getConnection().createStatement();
            statement.execute(query);
        } catch (SQLException e) {
            e.printStackTrace();
            result = false;
        }

        return result;
    }

    @Override
    public Table createTargetTable(Table table, String catalog) {
        List<Field> fields = table.getFields();
        Table table1 = new Table(catalog, table.getSchema(), table.getName());
//        dropTableIfExist(table1);
        log.info("Creating table :" + table1.getName());
        try {
            StringBuilder query = new StringBuilder("CREATE TABLE " + buildTableName(table1) + "(");
            for (int i = 0, l = fields.size(); i < l; i++) {
                Field field = fields.get(i);
                query.append(field.getName()).append(" ").append(field.getType()); //todo append not null here if its not null
                if (i < l - 1) {
                    query.append(",");
                }
            }
            query.append(") ");
            log.info("Create Query for '" + table1.getName() + "' :-->" + query);
            PreparedStatement pe = createPreparedStatement(query.toString());
            pe.execute();
        } catch (SQLException ex) {
            log.error("Error for creating table '" + table1.getName() + "'");
            ex.printStackTrace();
            return null;
        }
        return table1;
    }

    private void dropTableIfExist(Table table) {
        String sqlQuery = "IF EXISTS DROP TABLE " + buildTableName(table);
        try {
            Statement statement = getConnection().createStatement();
            statement.execute(sqlQuery);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
    }

    private PreparedStatement createPreparedStatement(String query) throws SQLException {
        return getConnection().prepareStatement(query);
    }
}
