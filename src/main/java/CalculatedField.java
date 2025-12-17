package com.example.calculator;

import java.lang.reflect.Field;

final class CalculatedField {
    final Field field;
    final Class<?> type;
    final Calculator<?, ?> calculator;

    CalculatedField(Field field, Calculator<?, ?> calculator) {
        this.field = field;
        this.type = field.getType();
        this.calculator = calculator;
        this.field.setAccessible(true);
    }
}