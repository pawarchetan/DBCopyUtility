package com.dbcopy.listener;

import com.dbcopy.model.Table;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ConsoleCopierListener implements CopierListener {

    private boolean outputPercentageOutput;
    private int percentageDone = 0;

    public ConsoleCopierListener(boolean outputPercentageOutput) {
        this.outputPercentageOutput = outputPercentageOutput;
    }

    @Override
    public void startCopyTable(Table table, long totalRows) {
        log.info("Copy table " + table.getName() + " (" + totalRows + " rows)");
        percentageDone = 0;
    }

    @Override
    public void copyTableStatus(Table table, long currentPos, long totalRows) {
        if (outputPercentageOutput) {
            int percentage = (int) (currentPos * 100 / totalRows);

            if (percentageDone + 10 < percentage) {
                log.info("Table '" + table.getName() + "' Progress: " + percentageDone + "%");
                percentageDone = percentage;
            }
        }
    }

    @Override
    public void error(Table table, Exception exception) {
        log.info("Error during copy table " + table + "!");
    }

    @Override
    public void endCopyTable(Table table) {
        if (outputPercentageOutput) {
            log.info("Table '" + table.getName() + "' Done : 100%");
        }
        log.info("===========================================");
    }

    @Override
    public void startCopy() {
        log.info("Start copy");
        log.info("===========================================");
    }

}
