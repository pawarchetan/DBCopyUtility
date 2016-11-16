package com.dbcopy.util;

import com.dbcopy.listener.CopierListener;
import com.dbcopy.model.Field;
import com.dbcopy.model.Table;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCopier implements Copier {

    private List<CopierListener> listeners = new ArrayList<>();
    private Database source = null;
    private Database target = null;

    public AbstractCopier(Database source, Database target) {
        super();
        this.source = source;
        this.target = target;
    }

    @Override
    public void copy() {
        if (checkConnections()) {
            fireStartCopy();
            Table table;

            while ((table = getNextTable()) != null) {
                long totalRows = source.countContentsForTable(table);
                Table targetTable = target.createTargetTable(table, "sis_new");
                fireStartCopyTable(table, totalRows);
                copyTableContents(table, targetTable);
                fireEndCopyTable(table);
            }
        } else {
            fireError(null, new Exception("Connection problems"));
        }
    }

    @Override
    public PreparedStatement setPreparedStatementParameters(PreparedStatement statement, Table table, ResultSet contents) throws Exception {
        List<Field> fields = table.getFields();
        for (int i = 0, l = fields.size(); i < l; i++) {
            Field field = fields.get(i);
            int parameterIndex = i + 1;
            Object value = contents.getObject(field.getName());
            if (value != null) {
                statement.setObject(parameterIndex, value, field.getIntType());
            } else {
                statement.setNull(parameterIndex, field.getIntType());
            }
        }
        return statement;
    }

    private boolean checkConnections() {
        boolean result = true;
        try {
            source.connect();
            target.connect();
        } catch (Exception e) {
            e.printStackTrace();
            result = false;
        }
        return result;
    }

    private void copyTableContents(Table table, Table targetTable) {
        int totalRowsOnSource = source.countContentsForTable(table);
        int totalProcessed = 0;

        try {
            PreparedStatement targetStatement = target.buildPreparedInsertStatement(table, targetTable);
            ResultSet sourceContents = source.getContentsForTable(table);

            while (sourceContents.next()) {
                totalProcessed++;
                fireCopyTableStatus(table, totalProcessed, totalRowsOnSource);

                targetStatement = setPreparedStatementParameters(targetStatement, table, sourceContents);
                targetStatement.execute();
            }
        } catch (Exception e) {
            fireError(table, e);
        }
    }

    @Override
    public void addCopierListener(CopierListener copierListener) {
        if (!listeners.contains(copierListener)) {
            listeners.add(copierListener);
        }
    }

    protected void fireStartCopyTable(Table table, long totalRows) {
        for (CopierListener listener : listeners) {
            listener.startCopyTable(table, totalRows);
        }
    }

    protected void fireCopyTableStatus(Table table, long currentPos, long totalRows) {
        for (CopierListener listener : listeners) {
            listener.copyTableStatus(table, currentPos, totalRows);
        }
    }

    protected void fireError(Table table, Exception error) {
        for (CopierListener listener : listeners) {
            listener.error(table, error);
        }
    }

    protected void fireEndCopyTable(Table table) {
        for (CopierListener listener : listeners) {
            listener.endCopyTable(table);
        }
    }

    protected void fireStartCopy() {
        listeners.forEach(com.dbcopy.listener.CopierListener::startCopy);
    }

}