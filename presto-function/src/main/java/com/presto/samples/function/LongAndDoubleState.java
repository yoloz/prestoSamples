package com.presto.samples.function;

import com.facebook.presto.spi.function.AccumulatorState;

/**
 * Created by ethan
 * 17-8-23
 */
public interface LongAndDoubleState extends AccumulatorState {

    default long getLong() {
        return 0;
    }

    void setLong(long value);

    default double getDouble() {
        return 0.0;
    }

    void setDouble(double value);
}
