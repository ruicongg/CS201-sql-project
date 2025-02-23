package edu.smu.smusql.parser;

import java.util.List;
import java.util.Map;

import edu.smu.smusql.interfaces.RowEntry;
public class WhereCondition {
    private String column;
    private String operator;
    private String value;
    private String logicalOperator; // AND/OR with next condition

    public WhereCondition(String column, String operator, String value) {
        this.column = column;
        this.operator = operator;
        this.value = value;
    }

    public void setLogicalOperator(String logicalOperator) {
        this.logicalOperator = logicalOperator;
    }


    public boolean evaluate(RowEntry row) {
        String columnValue = row.getValue(column);
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

    public String getOperator() {
        return operator;
    }

    public boolean isExactMatch() {
        return operator.equals("=");
    }

    public String getColumn() {
        return column;
    }

    public String getValue() {
        return value;
    }
    public String getLogicalOperator() {
        return logicalOperator;
    }

}