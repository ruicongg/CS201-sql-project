package edu.smu.smusql.evaluator;

/**
 * Evaluator is responsible for evaluating the performance of a database by executing
 * a mix of different types of SQL queries (INSERT, DELETE, UPDATE, SELECT) based on
 * specified percentages.
 * @author gav
 */
public class Evaluator {

    private final double insertPercentage;
    private final double deletePercentage;
    private final double updatePercentage;
    private final double selectPercentage;

    /**
     * Private constructor to enforce object creation through the Builder.
     *
     * @param builder The Builder instance containing the configuration.
     */
    private Evaluator(Builder builder) {
        this.insertPercentage = builder.insertPercentage;
        this.deletePercentage = builder.deletePercentage;
        this.updatePercentage = builder.updatePercentage;
        this.selectPercentage = builder.selectPercentage;
    }

    // Getters for each query type percentage

    public double getInsertPercentage() {
        return insertPercentage;
    }

    public double getDeletePercentage() {
        return deletePercentage;
    }

    public double getUpdatePercentage() {
        return updatePercentage;
    }

    public double getSelectPercentage() {
        return selectPercentage;
    }

    /**
     * Builder class for Evaluator. Allows setting percentages for different query types.
     */
    public static class Builder {
        private double insertPercentage = 0.0;
        private double deletePercentage = 0.0;
        private double updatePercentage = 0.0;
        private double selectPercentage = 0.0;

        /**
         * Sets the percentage for INSERT queries.
         *
         * @param insertPercentage Percentage of INSERT queries (0-100).
         * @return The Builder instance.
         */
        public Builder insertPercentage(double insertPercentage) {
            this.insertPercentage = insertPercentage;
            return this;
        }

        /**
         * Sets the percentage for DELETE queries.
         *
         * @param deletePercentage Percentage of DELETE queries (0-100).
         * @return The Builder instance.
         */
        public Builder deletePercentage(double deletePercentage) {
            this.deletePercentage = deletePercentage;
            return this;
        }

        /**
         * Sets the percentage for UPDATE queries.
         *
         * @param updatePercentage Percentage of UPDATE queries (0-100).
         * @return The Builder instance.
         */
        public Builder updatePercentage(double updatePercentage) {
            this.updatePercentage = updatePercentage;
            return this;
        }

        /**
         * Sets the percentage for SELECT queries.
         *
         * @param selectPercentage Percentage of SELECT queries (0-100).
         * @return The Builder instance.
         */
        public Builder selectPercentage(double selectPercentage) {
            this.selectPercentage = selectPercentage;
            return this;
        }

        /**
         * Validates the percentages and builds an Evaluator instance.
         *
         * @return A new Evaluator instance.
         * @throws IllegalArgumentException If the total percentage does not equal 100.
         */
        public Evaluator build() {
            double total = insertPercentage + deletePercentage + updatePercentage + selectPercentage;
            if (Math.abs(total - 100.0) > 0.0001) { // Allowing a tiny margin for floating point precision
                throw new IllegalArgumentException(
                        String.format("Total percentage must be 100. Current total: %.2f", total));
            }

            if (insertPercentage < 0 || deletePercentage < 0 || updatePercentage < 0 || selectPercentage < 0) {
                throw new IllegalArgumentException("No negative values allowed.");
            }

            return new Evaluator(this);
        }
    }

    @Override
    public String toString() {
        return "Evaluator{" +
                "insertPercentage=" + insertPercentage +
                ", deletePercentage=" + deletePercentage +
                ", updatePercentage=" + updatePercentage +
                ", selectPercentage=" + selectPercentage +
                '}';
    }

}
