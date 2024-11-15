package edu.smu.smusql.bplus;

import edu.smu.smusql.interfaces.StorageInterface;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.smu.smusql.bplus.BPlusTree;
import edu.smu.smusql.interfaces.RowEntry;
import edu.smu.smusql.interfaces.StorageInterface;
import edu.smu.smusql.parser.*;

import java.util.*;

public class BPlusTreeStorage implements StorageInterface {

    private Map<String, BPlusTreeTable> tables;

    public BPlusTreeStorage() {
        tables = new HashMap<>();
    }
    
    @Override
    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }

    @Override
    public void insert(Insert insert) {
        BPlusTreeTable table = tables.get(insert.getTablename());
        table.addRow(createRowMap(table.getColumns(), insert.getValues()));
    }
    @Override
    public List<String> getColumns(String tableName) {
        return tables.get(tableName).getColumns();
    }

    @Override
    public int getColumnCount(String tableName) {
        return tables.get(tableName).getColumns().size();
    }

    @Override 
    public int delete(Delete delete) {
        BPlusTreeTable table = tables.get(delete.getTablename());
        if (delete.getConditions().size() == 1) {
            List<RowEntry> rowEntries = processOneWhereConditions(delete.getConditions().get(0), table);
            for (RowEntry rowEntry : rowEntries) {
                rowEntry.setDeleted();
            }
            return rowEntries.size();
        }
        if (delete.getConditions().size() == 2) {
            List<RowEntry> rowEntries = processTwoWhereConditions(delete.getConditions(), table);

            for (RowEntry rowEntry : rowEntries) {
                rowEntry.setDeleted();
            }
            return rowEntries.size();
        }
        return 0;
    }

    
    @Override
    public List<RowEntry> select(Select select) {
        BPlusTreeTable table = tables.get(select.getTablename());
        if (select.getConditions().size() == 0) {
            return table.getAllEntries();
        }
        if (select.getConditions().size() == 1) {
            return processOneWhereConditions(select.getConditions().get(0), table);
        }
        return processTwoWhereConditions(select.getConditions(), table);
    }




    @Override
    public int update(Update update) {
        BPlusTreeTable table = tables.get(update.getTablename());
        if (update.getConditions().size() == 1) {
            List<RowEntry> rowEntries = processOneWhereConditions(update.getConditions().get(0), table);
            for (RowEntry rowEntry : rowEntries) {
                rowEntry.addOrUpdateValue(update.getColumnname(), update.getValue());
            }
            return rowEntries.size();
        }
        if (update.getConditions().size() == 2) {
            List<RowEntry> rowEntries = processTwoWhereConditions(update.getConditions(), table);
            for (RowEntry rowEntry : rowEntries) {
                rowEntry.addOrUpdateValue(update.getColumnname(), update.getValue());
            }
            return rowEntries.size();
        }
        return 0;
    }


    @Override
    public void create(Create create) {
        tables.put(create.getTablename(), new BPlusTreeTable(create.getColumns()));
    }


    /*
     * HELPER METHODS
     */

     private RowEntry createRowMap(List<String> columns, List<String> values) {
        RowEntry row = new RowEntry();
        for (int i = 0; i < columns.size(); i++) {
            row.addOrUpdateValue(columns.get(i), values.get(i));
        }
        return row;
    }


    private List<RowEntry> processOneWhereConditions(WhereCondition whereCondition, BPlusTreeTable table) {
        String column = whereCondition.getColumn();
        String operator = whereCondition.getOperator();
        String value = whereCondition.getValue();

        switch (operator) {
            case "=":
                return table.getTreeForColumn(column).searchEqualTo(value);
            case ">":
                return table.getTreeForColumn(column).searchGreaterThan(value);
            case "<":
                return table.getTreeForColumn(column).searchLessThan(value);
            case ">=":
                return table.getTreeForColumn(column).searchGreaterThanOrEqualTo(value);
            case "<=":
                return table.getTreeForColumn(column).searchLessThanOrEqualTo(value);
            default:
                return new ArrayList<RowEntry>();
        }
    }

    private List<RowEntry> processTwoWhereConditions(List<WhereCondition> whereConditions, BPlusTreeTable table) {
        List<RowEntry> condition1 = processOneWhereConditions(whereConditions.get(0), table);
        List<RowEntry> condition2 = processOneWhereConditions(whereConditions.get(1), table);
        if (whereConditions.get(0).getLogicalOperator().equals("AND")) {
            return intersectLists(condition1, condition2);
        }
        return unionLists(condition1, condition2);
    }

    private List<RowEntry> intersectLists(List<RowEntry> list1, List<RowEntry> list2) {
        return list1.stream().filter(list2::contains).collect(Collectors.toList());
    }

    private List<RowEntry> unionLists(List<RowEntry> list1, List<RowEntry> list2) {
        return Stream.concat(list1.stream(), list2.stream())
                .distinct()
                .collect(Collectors.toList());
    }
}
