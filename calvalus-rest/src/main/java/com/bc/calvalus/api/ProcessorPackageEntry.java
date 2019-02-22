package com.bc.calvalus.api;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
@XmlRootElement
public class ProcessorPackageEntry {

    private static final String SYSTEM_PATH = "/calvalus/software/1.0";
    private static final String USER_HOME = "/calvalus/home";

    private String name;
    private String version;
    private String owner;
    private String path;

    public ProcessorPackageEntry() {}

    public ProcessorPackageEntry(String name, String version, String owner, String path) {
        this.name = name;
        this.version = version;
        this.owner = owner;
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getOwner() { return owner; }

    public void setOwner(String owner) { this.owner = owner; }

    public String getPath() { return path != null ? path : owner == null ? SYSTEM_PATH + "/" + name + "-" + version : USER_HOME + "/" + owner + "/software/" + name + "-" + version; }

    public void setPath(String path) { this.path = path; }

    public String id() {
        return String.format("%s-%s", name, version);
    }

    @Override
    public String toString() { return String.format("ProcessorBundle %s %s %s", name, version, path); }
}
