package com.bc.calvalus.api;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Properties;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
@XmlRootElement
public class CalvalusHadoopRequestEntry {

    private static final String SYSTEM_PATH = "/calvalus/software/1.0";
    private static final String USER_HOME = "/calvalus/home";

    private String id;
    private String json;
    private Properties features;

    public CalvalusHadoopRequestEntry() {}

    public CalvalusHadoopRequestEntry(String id, String json, Properties features) {
        this.id = id;
        this.json = json;
        this.features = features;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public Properties getFeatures() { return features; }

    public void setFeatures(Properties features) { this.features = features; }

    @Override
    public String toString() { return String.format("CalvalusHadoopRequest %s", id); }
}
