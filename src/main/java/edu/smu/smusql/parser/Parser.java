package edu.smu.smusql.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.smu.smusql.WhereCondition;

public class Parser {

    /*
     * Use instanceof to sort the output
     */
    public static Object parseStatement(String query) {
        String[] tokens = query.trim().split("\\s+");
        String command = tokens[0].toUpperCase();

        return switch (command) {
            case "CREATE" -> parseCreate(tokens);
            case "SELECT" -> parseSelect(tokens);
            case "UPDATE" -> parseUpdate(tokens);
            case "INSERT" -> parseInsert(tokens);
            case "DELETE" -> parseDelete(tokens);
            default -> "ERROR: Unknown command";
        };
    }

    private static List<WhereCondition> getConditions(int start, String[] tokens) {
        int idx = start;
        List<WhereCondition> conditions = new ArrayList<>();
        while (idx < tokens.length) {
            WhereCondition condition = new WhereCondition(tokens[idx++], tokens[idx++], tokens[idx++]);
            if (idx < tokens.length) { // check whether this is last condition
                condition.setLogicalOperator(tokens[idx++]);
            }
            conditions.add(condition);
        }
        return conditions;
    }

    private static List<String> getColumns(int start, String[] tokens) {
        List<String> columns = new ArrayList<>();
        for (int i = start; i < tokens.length; i++) {
            columns.add(tokens[i].trim().replaceAll("\\(", "").replaceAll("\\)", ""));
        }
        return columns;
    }

    private static Create parseCreate(String[] tokens) {
        // CREATE TABLE student (id, name, age, gpa, deans_list)
        String tablename = tokens[2];

        return new Create(tablename, getColumns(3, tokens));
    }

    public static Select parseSelect(String[] tokens) {
        // SELECT * FROM student WHERE gpa > 3.8 AND age < 20
        String tablename = tokens[3];
        return new Select(tablename, getConditions(4, tokens));
    }

    public static Update parseUpdate(String[] tokens) {
        // UPDATE student SET deans_list = True WHERE gpa > 3.8 OR age = 201
        String tablename = tokens[1];
        String columnname = tokens[2];
        String value = tokens[4];

        return new Update(tablename, columnname, value, getConditions(6, tokens));
    }

    public static Insert parseInsert(String[] tokens) {
        // INSERT INTO student VALUES (1, John, 30, 2.4, False)
        String tablename = tokens[2];
        return new Insert(tablename, getColumns(4, tokens));
    }

    private static Delete parseDelete(String[] tokens) {
        // DELETE FROM student WHERE gpa < 2.0 OR name = little_bobby_tables
        String tablename = tokens[2];

        return new Delete(tablename, getConditions(4, tokens));
    }


}