package com.presto.samples.function;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Created by ethan
 * 17-8-23
 */
public class FunctionsPlugin implements Plugin {

    @Override
    public Set<Class<?>> getFunctions() {
        return ImmutableSet.<Class<?>>builder()
                .add(IsStringNullFunction.class)
                .add(LowerStringFunction.class)
                .add(AverageAggregation.class)
                .add(FieldEqualFunction.class)
                .build();
    }
}
