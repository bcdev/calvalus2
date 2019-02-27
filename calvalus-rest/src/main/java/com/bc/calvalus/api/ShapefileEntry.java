package com.bc.calvalus.api;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ShapefileEntry {

    private String owner;
    private String path;

    // necessary for Jersey
    public ShapefileEntry() {
    }

    public ShapefileEntry(String owner, String path) {
        this.owner = owner;
        this.path = path;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return "ShapefileEntry{" + "owner='" + owner + '\'' + ", path='" + path + '\'' + '}';
    }
}
