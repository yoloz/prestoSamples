package com.presto.samples.function;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

/**
 * Created by ethan
 * 17-8-23
 */
@ScalarFunction("custom_lowercaser")
@Description("converts the string to alternating case")
public class LowerStringFunction {


    @SqlType(StandardTypes.VARCHAR)
    public static Slice lowercaser(@SqlType(StandardTypes.VARCHAR) Slice slice) {
        String argument = slice.toStringUtf8();
        return Slices.utf8Slice(argument.toLowerCase());
    }
}
