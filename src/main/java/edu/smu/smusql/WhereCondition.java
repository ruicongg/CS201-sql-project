package edu.smu.smusql;

import java.util.List;
import java.util.Map;

public class WhereCondition {
    String column;
    String operator;
    String value;
    String logicalOperator; // AND/OR with next condition

    public WhereCondition(String column, String operator, String value) {
        this.column = column;
        this.operator = operator;
        this.value = value;
    }

    public void setLogicalOperator(String logicalOperator) {
        this.logicalOperator = logicalOperator;
    }


    public boolean evaluate(Map<String, String> row) {
        String columnValue = row.get(column);
        if (columnValue == null) {
            return false;
        }

        columnValue = columnValue.trim();
        
        if (isNumeric(columnValue) && isNumeric(value)) {
            double columnNumber = Double.parseDouble(columnValue);
            double valueNumber = Double.parseDouble(value);
            
            return compareNumeric(columnNumber, valueNumber);
        }
        
        return compareString(columnValue, value);
    }

    private boolean compareNumeric(double columnNumber, double valueNumber) {
        return switch (operator.toUpperCase()) {
            case "=" -> columnNumber == valueNumber;
            case ">" -> columnNumber > valueNumber;
            case "<" -> columnNumber < valueNumber;
            case ">=" -> columnNumber >= valueNumber;
            case "<=" -> columnNumber <= valueNumber;
            case "!=" -> columnNumber != valueNumber;
            default -> false;
        };
    }

    private boolean compareString(String columnValue, String targetValue) {
        return switch (operator.toUpperCase()) {
            case "=" -> columnValue.equals(targetValue);
            case ">" -> columnValue.compareTo(targetValue) > 0;
            case "<" -> columnValue.compareTo(targetValue) < 0;
            case ">=" -> columnValue.compareTo(targetValue) >= 0;
            case "<=" -> columnValue.compareTo(targetValue) <= 0;
            case "!=" -> !columnValue.equals(targetValue);
            default -> false;
        };
    }

    private boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    

}