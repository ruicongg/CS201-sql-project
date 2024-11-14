package edu.smu.smusql.bst;

import edu.smu.smusql.table.BSTStorage;
import edu.smu.smusql.parser.Create;
import edu.smu.smusql.parser.Insert;
import edu.smu.smusql.parser.Select;
import edu.smu.smusql.interfaces.RowEntry;


import java.util.List;

public class BSTStorageTest {
    public static void main(String[] args) {
        BSTStorage storage = new BSTStorage();

        // Create table
        System.out.println("Creating table 'users'...");
        storage.create(new Create("users", List.of("id", "name", "age")));

        // Insert rows
        System.out.println("Inserting rows into 'users' table...");
        storage.insert(new Insert("users", List.of("id", "name", "age"), List.of("1", "Alice", "30")));
        storage.insert(new Insert("users", List.of("id", "name", "age"), List.of("2", "Bob", "25")));

        // Select rows
        System.out.println("Selecting rows from 'users' table...");
        List<RowEntry> rows = storage.select(new Select("users", List.of()));
        for (RowEntry row : rows) {
            System.out.println(row);
        }
    }
}