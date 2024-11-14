package edu.smu.smusql.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
            default -> throw new InvalidCommandException("ERROR: Unknown command");
        };
    }

    private static List<WhereCondition> getConditions(int start, String[] tokens) {
        int idx = start;
        List<WhereCondition> conditions = new ArrayList<>();
        while (idx < tokens.length) {
            WhereCondition condition = new WhereCondition(tokens[idx++], tokens[idx++], tokens[idx++]);
            if (idx < tokens.length) { // check whether this is last condition
                // if there is another token, assume it is a logical operator
                condition.setLogicalOperator(tokens[idx++].toUpperCase());
            }
            conditions.add(condition);
        }
        return conditions;
    }

    private static List<String> getColumns(int start, String[] tokens) {
        List<String> columns = new ArrayList<>();
        for (int i = start; i < tokens.length; i++) {
            columns.add(tokens[i].trim().replaceAll("[,()]", ""));
        }
        return columns;
    }

    private static Create parseCreate(String[] tokens) {
        // CREATE TABLE student (id, name, age, gpa, deans_list)
        if (!tokens[1].equalsIgnoreCase("TABLE")) {
            throw new InvalidCommandException("ERROR: Invalid CREATE TABLE syntax");
        }
        String tablename = tokens[2];
        return new Create(tablename, getColumns(3, tokens));
    }

    public static Select parseSelect(String[] tokens) {
        // SELECT * FROM student WHERE gpa > 3.8 AND age < 20, note this only handles select *
        if (!tokens[1].equals("*") || !tokens[2].toUpperCase().equals("FROM")) {
            throw new InvalidCommandException("ERROR: Invalid SELECT syntax");
        }
        String tablename = tokens[3];
        return new Select(tablename, getConditions(5, tokens));
    }

    public static Update parseUpdate(String[] tokens) {
        // UPDATE student SET deans_list = True WHERE gpa > 3.8 OR age = 201
        if (tokens.length < 6 || !tokens[2].toUpperCase().equals("SET") || !tokens[4].equals("=")) {
            throw new InvalidCommandException("ERROR: Invalid UPDATE syntax");
        }
        String tablename = tokens[1];
        String columnname = tokens[3];
        String value = tokens[5];
        return new Update(tablename, columnname, value, getConditions(7, tokens));
    }

    public static Insert parseInsert(String[] tokens) {
        // INSERT INTO student VALUES (1, John, 30, 2.4, False)
        if (tokens.length < 5 || !tokens[1].toUpperCase().equals("INTO")) {
            throw new InvalidCommandException("ERROR: Invalid INSERT INTO syntax");
        }
        String tablename = tokens[2];
        return new Insert(tablename, getColumns(4, tokens));
    }

    private static Delete parseDelete(String[] tokens) {
        // DELETE FROM student WHERE gpa < 2.0 OR name = little_bobby_tables
        if (!tokens[1].toUpperCase().equals("FROM")) {
            throw new InvalidCommandException("ERROR: Invalid DELETE syntax");
        }
        String tablename = tokens[2];

        return new Delete(tablename, getConditions(4, tokens));
    }


}