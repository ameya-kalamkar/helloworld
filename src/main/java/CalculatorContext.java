package com.example.calculator;

public interface CalculatorContext<I> {
    
    /**
     * Get the Input Data
     */
    I getInput();

    /**
     * Get a registered Output Attribute (POJO Field).
     * @param attributeName The field name (use Lombok Constants for safety).
     */
    <T> T get(String attributeName);

    /**
     * Execute a "Support Calculator" dynamically.
     * The result is CACHED for this session.
     * @param calculatorClass The class of the stateless calculator to run.
     * @return The result of the calculation.
     */
    <R> R calculate(Class<? extends Calculator<I, R>> calculatorClass);
}