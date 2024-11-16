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
            List<Integer> indices = processOneWhereConditions(delete.getConditions().get(0), table);
            int numDeleted = 0;
            for (Integer index : indices) {
                table.getRow(index).setDeleted();
                numDeleted++;
            }
            return numDeleted;
        }
        if (delete.getConditions().size() == 2) {
            List<Integer> indices = processTwoWhereConditions(delete.getConditions(), table);
            int numDeleted = 0;
            for (Integer index : indices) {
                table.getRow(index).setDeleted();
                numDeleted++;
            }
            return numDeleted;
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
            List<Integer> indices = processOneWhereConditions(select.getConditions().get(0), table);
            List<RowEntry> rowEntries = new ArrayList<>();
            for (Integer index : indices) {

                if (!table.getRow(index).isDeleted()) {
                    rowEntries.add(table.getRow(index));
                }
            }
            return rowEntries;
        }
        List<Integer> indices = processTwoWhereConditions(select.getConditions(), table);
        List<RowEntry> rowEntries = new ArrayList<>();
        for (Integer index : indices) {

            if (!table.getRow(index).isDeleted()) {
                rowEntries.add(table.getRow(index));
            }
        }
        return rowEntries;
    }




    @Override
    public int update(Update update) {
        BPlusTreeTable table = tables.get(update.getTablename());
        if (update.getConditions().size() == 1) {
            List<Integer> indices = processOneWhereConditions(update.getConditions().get(0), table);
            int numUpdated = 0;
            for (Integer index : indices) {
                if (!table.getRow(index).isDeleted()) {
                    table.updateRow(index, update.getColumnname(), update.getValue());
                    numUpdated++;
                }
            }
            return numUpdated;
        }
        if (update.getConditions().size() == 2) {
            List<Integer> indices = processTwoWhereConditions(update.getConditions(), table);
            int numUpdated = 0;
            for (Integer index : indices) {
                if (!table.getRow(index).isDeleted()) {
                    table.updateRow(index, update.getColumnname(), update.getValue());
                    numUpdated++;
                }
            }
            return numUpdated;
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


    private List<Integer> processOneWhereConditions(WhereCondition whereCondition, BPlusTreeTable table) {
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
                return new ArrayList<Integer>();
        }
    }

    private List<Integer> processTwoWhereConditions(List<WhereCondition> whereConditions, BPlusTreeTable table) {
        List<Integer> condition1 = processOneWhereConditions(whereConditions.get(0), table);
        List<Integer> condition2 = processOneWhereConditions(whereConditions.get(1), table);
        if (whereConditions.get(0).getLogicalOperator().equals("AND")) {
            return intersectLists(condition1, condition2);
        }
        return unionLists(condition1, condition2);
    }

    private List<Integer> intersectLists(List<Integer> list1, List<Integer> list2) {
        return list1.stream().filter(list2::contains).collect(Collectors.toList());
    }

    private List<Integer> unionLists(List<Integer> list1, List<Integer> list2) {
        return Stream.concat(list1.stream(), list2.stream())
                .distinct()
                .collect(Collectors.toList());
    }
}
