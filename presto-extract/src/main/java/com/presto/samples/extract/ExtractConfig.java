package com.presto.samples.extract;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

/**
 * Created by ethan
 * 17-10-18
 */
public class ExtractConfig {


    private String jdbcUrl = "jdbc:mysql://10.68.23.14:3306/";
    private String userName = "root";
    private String pwd = "root";


    @NotNull
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    @NotNull
    public String getUserName() {
        return userName;
    }

    @NotNull
    public String getPwd() {
        return pwd;
    }


    @Config("extract.jdbc-url")
    public ExtractConfig setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    @Config("extract.jdbc-name")
    public ExtractConfig setUserName(String userName) {
        this.userName = userName;
        return this;
    }

    @Config("extract.jdbc-pwd")
    public ExtractConfig setPwd(String pwd) {
        this.pwd = pwd;
        return this;
    }
}
