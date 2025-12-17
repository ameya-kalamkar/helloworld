package com.example.calculator;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface CalculatedBy {
    Class<? extends Calculator<?, ?>> calculator();
}