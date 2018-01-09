package com.presto.samples.extract;

import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;

/**
 * Created by ethan
 * 17-10-20
 */
public class ExtractTableProperties {

    //name必须小写，否则报错Invalid session property name “xxx”，PropertyMetadata会判断
    public static final String LOCATION_PROPERTY = "location";
    public static final String CUSTOM_MAP = "map_json";
    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public ExtractTableProperties() {
        tableProperties = ImmutableList.of(
                stringSessionProperty(
                        LOCATION_PROPERTY,
                        "location URI for create table",
                        null,
                        false),
                stringSessionProperty(
                        CUSTOM_MAP,
                        "custom json properties",
                        null,
                        false)
        );
    }

    public static String getLocationProperty() {
        return LOCATION_PROPERTY;
    }

    public static String getCustomMap() {
        return CUSTOM_MAP;
    }

    public List<PropertyMetadata<?>> getTableProperties() {
        return tableProperties;
    }
}
