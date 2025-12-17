package com.example.calculator;

import java.util.function.Function;

/**
 * Version 3.1: Stream Compatible
 * Represents a stateless unit of logic.
 * Extends Function to allow usage in Java 8 Streams.
 * * Input (T) = CalculatorContext<I>
 * Output (R) = R
 */
@FunctionalInterface
public interface Calculator<I, R> extends Function<CalculatorContext<I>, R> {
    
    // This overrides Function.apply but keeps the exact same signature
    @Override
    R apply(CalculatorContext<I> ctx);
}