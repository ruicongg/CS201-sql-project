package edu.smu.smusql;

import java.util.*;
import java.util.function.Function;

import edu.smu.smusql.interfaces.RowEntry;
import edu.smu.smusql.parser.*;
import edu.smu.smusql.table.Table;
import edu.smu.smusql.interfaces.StorageInterface;
import edu.smu.smusql.table.IndicesStorage;

public class Engine {

    /**
     * CHANGE THIS FOR STORAGE IMPLEMENTATIONS
     */
    private final StorageInterface storageInterface = new IndicesStorage();

    /**
     * REMOVE PARAMETERS TO DISABLE BLOOM FILTER
     * @param size
     * @param hashCount
     */
    private final BloomFilter bloomFilter = new BloomFilter(640000, 2);
    
    public String executeSQL(String query) {
        /*
         * Basic Input Validation
         */
        try {
            if (query == null || query.length() == 0) {
                throw new InvalidCommandException("ERROR: No command found");
            }
            Object parsedStatement = Parser.parseStatement(query);
            
            if (parsedStatement instanceof Create create) {
                return create(create);
            } else if (parsedStatement instanceof Insert insert) {
                return insert(insert);
            } else if (parsedStatement instanceof Select select) {
                return select(select);
            } else if (parsedStatement instanceof Update update) {
                return update(update);
            } else if (parsedStatement instanceof Delete delete) {
                return delete(delete);
            } else {
                throw new InvalidCommandException("ERROR:Unsupported command type");
            }
        } catch (InvalidCommandException e) {
            return e.getMessage();
        }
    }

    public String insert(Insert insert) {

        String tableName = insert.getTablename();
        if (!storageInterface.tableExists(tableName)) {
            throw new InvalidCommandException("ERROR: Table not found");
        }

        List<String> values = insert.getValues();
        if (values.size() != storageInterface.getColumnCount(tableName)) {
            throw new InvalidCommandException("ERROR: Column count doesn't match value count");
        }
        /*
         * Add into bloom filter
         */
        if (bloomFilter.getSize() != 0) {
            for (String value: values) {
                bloomFilter.add(value);
            }
        }
        

        storageInterface.insert(insert);
        return "Row inserted into " + tableName;
    }

    public String delete(Delete delete) {
        
        String tableName = delete.getTablename();
        if (!storageInterface.tableExists(tableName)) {
            throw new InvalidCommandException("ERROR: Table not found");
        }

        List<WhereCondition> conditions = delete.getConditions();
        if (bloomFilter.getSize() != 0 && conditionsBloomFilter(conditions)) {
            return "No records matched for deletion (filtered by Bloom filter).";
        }

        int deletedCount = storageInterface.delete(delete);
        return "Rows deleted from " + tableName + ". " + deletedCount + " rows affected.";
        
    }

    public String select(Select select) {
    
        String tableName = select.getTablename();
        if (!storageInterface.tableExists(tableName)) {
            throw new InvalidCommandException("ERROR: Table not found");
        }

        List<WhereCondition> conditions = select.getConditions();
        if (bloomFilter.getSize() != 0 && conditionsBloomFilter(conditions)) {
            return "No matching records found (filtered by Bloom filter).";
        }

        List<RowEntry> rows = storageInterface.select(select);
        return formatTableOutput(storageInterface.getColumns(tableName), rows);
    }

    public String update(Update update) {


        String tableName = update.getTablename();
        if (!storageInterface.tableExists(tableName)) {
            throw new InvalidCommandException("ERROR: Table not found");
        }

        if (!storageInterface.getColumns(tableName).contains(update.getColumnname())) {
            throw new InvalidCommandException("ERROR: Column not found");
        }

        List<WhereCondition> conditions = update.getConditions();
        if (bloomFilter.getSize() != 0 && conditionsBloomFilter(conditions)) {
            return "No records matched for update (filtered by Bloom filter).";
        }

        int updatedCount = storageInterface.update(update);


        return String.format("Table %s updated. %d rows affected.", tableName, updatedCount);
    }

    public String create(Create create) {
    
        String tableName = create.getTablename();
        if (storageInterface.tableExists(tableName)) {
            throw new InvalidCommandException("ERROR: Table already exists");
        }

        storageInterface.create(create);

        return "Table " + tableName + " created";
    }

    /*
     * HELPER METHODS
     */
    private boolean conditionsBloomFilter(List <WhereCondition> conditions) {
        if (conditions != null && !conditions.isEmpty()) {
            boolean mightContainAnyCondition = conditions.stream()
                .map(WhereCondition::getValue)
                .anyMatch(bloomFilter::mightContain);

            return (!mightContainAnyCondition); // returns true if no condition found
        }
        return false; // returns false by default
    }

    private String formatTableOutput(List<String> columns, List<RowEntry> rows) {
        StringBuilder result = new StringBuilder();
        // Headers
        result.append(String.join("\t", columns))
              .append("\n");
        
        // Rows

        for (RowEntry row : rows) {
            result.append(row.getValue(columns.get(0)));
        }
        for (RowEntry row : rows) {
            for (int i = 1; i < columns.size(); i++) {
                result.append("\t");
                result.append(row.getValue(columns.get(i)));
            }
            result.append("\n");
        }
        
        return result.toString();
    }


}
