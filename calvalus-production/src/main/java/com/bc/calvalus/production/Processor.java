package com.bc.calvalus.production;

public class Processor {
    private String operator;
    private String name;
    private String defaultParameters;
    private String bundle;
    private String[] versions;

    public Processor(String operator, String name, String bundle, String[] versions) {
        this.operator = operator;
        this.name = name;
        this.bundle = bundle;
        this.versions = versions;
    }

    public Processor(String operator, String name, String defaultParameters, String bundle, String[] versions) {
        this.operator = operator;
        this.name = name;
        this.defaultParameters = defaultParameters;
        this.bundle = bundle;
        this.versions = versions;
    }

    public String getOperator() {
        return operator;
    }

    public String getName() {
        return name;
    }

    public String getDefaultParameters() {
        return defaultParameters;
    }

    public String getBundle() {
        return bundle;
    }

    public String[] getVersions() {
        return versions;
    }

    public static class Bundle {
        String name;
        String version;

        public Bundle(String name, String version) {
            this.name = name;
            this.version = version;
        }
    }
}
