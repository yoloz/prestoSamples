package com.presto.samples.function;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.charset.Charset;
import java.util.Objects;

/**
 * 参数中的参数类型要统一(即不可一个string，一个int等)
 * 参数中Integer不支持
 */

@ScalarFunction("custom_fieldEqual")
@Description("Returns original field and sys_out if field meet condition")
public class FieldEqualFunction {

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fieldEqual(@SqlNullable @SqlType("T") Slice value, @SqlNullable @SqlType("T") Slice conditional) {
        if (value == null) return Slices.utf8Slice("null");
        if (conditional == null) return value;
        if (Objects.equals(value, conditional))
            System.out.println(value.toString(Charset.forName("utf-8")));
        return value;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fieldEqual(@SqlNullable @SqlType("T") Long value, @SqlNullable @SqlType("T") Long conditional) {
        if (value == null) return Slices.utf8Slice("null");
        if (conditional == null) return Slices.utf8Slice(value + "");
        if (Objects.equals(value, conditional))
            System.out.println(String.valueOf(value));
        return Slices.utf8Slice(String.valueOf(value));
    }

    /* TODO 类似concat函数有问题，即多参数值的自定义函数
    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fieldEqual(@SqlNullable @SqlType("T") Slice... value) {
        if (value == null) return Slices.utf8Slice("false");
        System.out.println(value.length);
        return Slices.utf8Slice("false");
    }*/

}
