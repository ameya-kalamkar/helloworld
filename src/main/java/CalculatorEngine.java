package com.example.calculator;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class CalculatorEngine<I, O> implements CalculatorContext<I> {

    // Metadata Cache: OutputClass -> (Field Name -> Definition)
    private static final Map<Class<?>, Map<String, CalculatedField>> METADATA_CACHE = new ConcurrentHashMap<>();

    private final I input;
    private final O output;
    private final Map<String, CalculatedField> registry;
    
    // Unified Cache: Stores results for both Fields ("tax") and Support Classes ("com...MyCalc")
    private final Map<String, Object> cache = new HashMap<>();
    
    // Cycle Detection Stack
    private final Set<String> currentlyComputing = new HashSet<>();

    public CalculatorEngine(I input, Class<O> outputClass) {
        this.input = input;
        try {
            this.output = outputClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Cannot instantiate " + outputClass, e);
        }
        this.registry = METADATA_CACHE.computeIfAbsent(outputClass, this::introspect);
    }

    public CalculatorEngine(I input, O outputInstance) {
        this.input = input;
        this.output = outputInstance;
        this.registry = METADATA_CACHE.computeIfAbsent(outputInstance.getClass(), this::introspect);
    }

    private Map<String, CalculatedField> introspect(Class<?> clazz) {
        Map<String, CalculatedField> map = new HashMap<>();
        for (Field field : clazz.getDeclaredFields()) {
            CalculatedBy ann = field.getAnnotation(CalculatedBy.class);
            if (ann == null) continue;

            try {
                Calculator<?, ?> calculator = ann.calculator().getDeclaredConstructor().newInstance();
                
                // Validate Return Type
                Class<?> applyReturnType = Arrays.stream(calculator.getClass().getMethods())
                        .filter(m -> m.getName().equals("apply") && !m.isBridge())
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("Calculator missing apply()"))
                        .getReturnType();

                if (!field.getType().isAssignableFrom(applyReturnType)) {
                    throw new IllegalStateException(
                        "Type mismatch: Field '" + field.getName() + "' is " + field.getType().getSimpleName() + 
                        " but Calculator returns " + applyReturnType.getSimpleName());
                }

                map.put(field.getName(), new CalculatedField(field, calculator));

            } catch (Exception e) {
                throw new RuntimeException("Failed to bind calculator to field: " + field.getName(), e);
            }
        }
        return map;
    }

    // --- CONTEXT IMPLEMENTATION ---

    @Override
    public I getInput() {
        return this.input;
    }

    @Override
    public <T> T get(String attributeName) {
        // 1. Resolve Definition
        CalculatedField cf = registry.get(attributeName);
        if (cf == null) throw new IllegalArgumentException("Unknown attribute: " + attributeName);

        // 2. Compute using unified helper
        return computeInternal(attributeName, () -> {
            // Check for Override (Pre-set value in POJO)
            try {
                Object preSet = cf.field.get(output);
                if (preSet != null) return preSet;
            } catch (IllegalAccessException e) { throw new RuntimeException(e); }

            // Execute
            Calculator<I, ?> calc = (Calculator<I, ?>) cf.calculator;
            Object val = calc.apply(this);
            
            // Type Check
            if (val != null && !cf.type.isInstance(val)) {
                throw new IllegalStateException("Calculator for '" + attributeName + "' returned wrong type: " + val.getClass().getName());
            }
            return val;
        });
    }

    @Override
    public <R> R calculate(Class<? extends Calculator<I, R>> calculatorClass) {
        // Key is the class name to avoid collisions with field names
        String key = calculatorClass.getName();
        
        return computeInternal(key, () -> {
            try {
                // Instantiate Stateless Calculator on-the-fly
                Calculator<I, R> calc = calculatorClass.getDeclaredConstructor().newInstance();
                return calc.apply(this);
            } catch (Exception e) {
                throw new RuntimeException("Failed to run support calculator: " + calculatorClass.getSimpleName(), e);
            }
        });
    }

    // --- CORE LOGIC: Memoization & Cycle Detection ---
    
    @SuppressWarnings("unchecked")
    private <T> T computeInternal(String key, Supplier<Object> computer) {
        // 1. Cache Hit
        if (cache.containsKey(key)) return (T) cache.get(key);

        // 2. Cycle Detection
        if (currentlyComputing.contains(key)) {
            throw new IllegalStateException("Circular dependency detected: " + key);
        }

        // 3. Compute & Cache
        try {
            currentlyComputing.add(key);
            T value = (T) computer.get();
            cache.put(key, value);
            return value;
        } finally {
            currentlyComputing.remove(key);
        }
    }

    public O compute() {
        registry.forEach((attr, cf) -> { 
            try { cf.field.set(output, get(attr)); } catch (Exception e) { throw new RuntimeException(e); } 
        });
        return output;
    }
}