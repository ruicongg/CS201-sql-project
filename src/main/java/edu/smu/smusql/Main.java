package edu.smu.smusql;

import java.util.*;

import edu.smu.smusql.evaluator.EvaluationMode;
import edu.smu.smusql.evaluator.EvaluationSetup;
import edu.smu.smusql.evaluator.Evaluator;
import edu.smu.smusql.evaluator.SupportedQueries;
import edu.smu.smusql.lsm.*;
import edu.smu.smusql.bst.*;

// @author ziyuanliu@smu.edu.sg

public class Main {
    /*
     * Main method for accessing the command line interface of the database engine.
     * MODIFICATION OF THIS FILE IS NOT RECOMMENDED!
     */
    static Engine dbEngine = new Engine();

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("1. Enter 'exit' to exit");
            System.out.println("2. Enter 'evaluate' to evaluate the engine");
            System.out.println("3. Enter 'lsm' to test the lsm implementation");
            System.out.print("4. Enter 'bst' to test the BST implementation \nsmusql> ");
            String query = scanner.nextLine();
            if (query.equalsIgnoreCase("exit")) {
                break;
            } else if (query.equalsIgnoreCase("evaluate")) {
                System.out.println("Please configure the initial database environment.");
                System.out.print("How many rows to prepopulate into USERS table? \nsmusql> ");
                int userRows = Integer.parseInt(scanner.nextLine());
                System.out.print("How many rows to prepopulate into PRODUCTS table? \nsmusql> ");
                int productRows = Integer.parseInt(scanner.nextLine());
                System.out.print("How many rows to prepopulate into ORDERS table? \nsmusql> ");
                int orderRows = Integer.parseInt(scanner.nextLine());

                EvaluationSetup evaluationSetup = new EvaluationSetup(dbEngine);
                dbEngine = evaluationSetup.setup(userRows, productRows, orderRows);

                System.out.println("How would you like to evaluate the engine?");
                System.out.println("1. Enter 'random' for random percentages of queries");
                System.out.print("2. Enter 'interactive' to select each percentages of queries \nsmusql> ");

                EvaluationMode mode = EvaluationMode.valueOf(scanner.nextLine().toUpperCase());
                Evaluator evaluator = buildEvaluator(scanner, mode, dbEngine);

                assert evaluator != null;

                System.out.println("Evaluator configured as " + evaluator);

                System.out.print("How many queries would you like to execute? \nsmusql> ");
                long numQueries = Long.parseLong(scanner.nextLine());
                System.out.println("What % of queries do you want to be complex? ");
                System.out.print("smusql> ");
                double complexPercentage = Double.parseDouble(scanner.nextLine()) / 100.0;

                long startTime = System.nanoTime();
                evaluator.evaluate(complexPercentage, numQueries);
                long stopTime = System.nanoTime();
                long elapsedTime = stopTime - startTime;
                double elapsedTimeInSecond = (double) elapsedTime / 1_000_000_000;
                System.out.println("Time elapsed: " + elapsedTimeInSecond + " seconds");
                break;
            } else if (query.equalsIgnoreCase("lsm")) {
                LSMTreeTester.testLSM();
                break;
            } else if (query.equalsIgnoreCase("bst")) {
                BSTTreeTester.testBST();
                break;
            }
            System.out.println(dbEngine.executeSQL(query));
        }
        scanner.close();
    }

    /**
     * Interactively builds an Evaluator instance by collecting percentages for each
     * query type.
     *
     * @param scanner  Scanner instance for reading user input.
     * @param dbEngine
     * @return Configured Evaluator instance or null if configuration failed.
     */
    private static Evaluator buildEvaluator(Scanner scanner, EvaluationMode mode, Engine dbEngine) {
        double[] percentages = new double[SupportedQueries.values().length];

        if (mode.equals(EvaluationMode.RANDOM)) {
            getRandomPercentages(percentages);
        } else if (mode.equals(EvaluationMode.INTERACTIVE)) {
            getPercentageInput(scanner, percentages);
        } else {
            throw new IllegalArgumentException("Unknown evaluation mode: " + mode);
        }

        printPercentages(percentages);

        try {
            return new Evaluator.Builder()
                    .insertPercentage(percentages[0])
                    .deletePercentage(percentages[1])
                    .updatePercentage(percentages[2])
                    .selectPercentage(percentages[3])
                    .dbEngine(dbEngine)
                    .build();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Prompts the user to enter a valid percentage for a given query type.
     *
     * @param scanner Scanner instance for reading user input.
     */
    private static void getPercentageInput(Scanner scanner, double[] percentages) {
        while (true) { // Loop until valid input is received
            double sum = 0.0;
            boolean validInput = true;

            // Iterate over each SupportedQuery to get user input
            for (int i = 0; i < SupportedQueries.values().length; i++) {
                double percentage = -1.0;

                // Prompt until a valid percentage is entered for the current query
                while (percentage < 0.0 || percentage > 100.0) {
                    System.out.print("Enter percentage for " + SupportedQueries.values()[i] + " queries: ");
                    String input = scanner.nextLine().trim();

                    if (input.equals("bye")) {
                        break;
                    }

                    try {
                        percentage = Double.parseDouble(input);

                        if (percentage < 0.0 || percentage > 100.0) {
                            System.out.println("Invalid input. Please enter a number between 0 and 100.");
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid input. Please enter a valid number.");
                        percentage = -1.0; // Reset to force re-entry
                    }
                }

                percentages[i] = percentage;
                sum += percentage;
            }

            // Check if the total sum of percentages is exactly 100%
            if (Math.abs(sum - 100.0) < 1e-6) { // Using a small epsilon for floating-point comparison
                break; // Valid input; exit the loop
            } else {
                System.out.println("\nThe total sum of percentages is " + sum + "%, which does not equal 100%.");
                System.out.println("Please re-enter all percentages.\n");
                // Optionally, reset the percentages array
                Arrays.fill(percentages, 0.0);
            }
        }
    }

    /**
     * Generates an array of random percentages that sum up to 100.
     *
     * @param percentages The percentage for each query types.
     * @return An array of random percentages.
     */
    private static void getRandomPercentages(double[] percentages) {
        if (percentages == null || percentages.length != SupportedQueries.values().length) {
            throw new IllegalArgumentException("Input array must be non-null and of size 4.");
        }

        Random random = new Random();

        // Generate three unique random numbers between 1 and 99
        int[] cuts = new int[3];
        for (int i = 0; i < 3; i++) {
            cuts[i] = random.nextInt(99) + 1; // [1, 99]
        }

        // Sort the cut points
        Arrays.sort(cuts);

        // Calculate the differences to get the percentages
        percentages[0] = cuts[0];
        percentages[1] = cuts[1] - cuts[0];
        percentages[2] = cuts[2] - cuts[1];
        percentages[3] = 100 - cuts[2];
    }

    private static void printPercentages(double[] percentages) {
        for (int i = 0; i < percentages.length; i++) {
            System.out.println(SupportedQueries.values()[i] + ":" + percentages[i] + "%");
        }
    }
}