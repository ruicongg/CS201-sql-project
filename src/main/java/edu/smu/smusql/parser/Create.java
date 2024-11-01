package edu.smu.smusql.parser;

import java.util.List;

/*
 * Object used for greater clarity of
 * required parameters for SQL statement
 */
public class Create {
    private String tablename;
    private List<String> columns;

    public Create(String tablename, List<String> columns) {
        this.tablename = tablename;
        this.columns = columns;
    }

    public String getTablename() {
        return tablename;
    }

    public List<String> getColumns() {
        return columns;
    }
}
