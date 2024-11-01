package edu.smu.smusql.parser;

public class InvalidCommandException extends RuntimeException {
    public InvalidCommandException(String message) {
        super(message);
    }
}
