package com.bc.calvalus.api;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;

@XmlRootElement
public class ShapefileEntry extends FileEntry {

    private String path;
    private String[] attributes = null;
    private String[][] features = null;

    // necessary for Jersey
    public ShapefileEntry() {}

    public ShapefileEntry(String name, String owner, Date modificationDate, long size, String path, String[] attributes, String[][] features) {
        super(name, owner, modificationDate, size);
        this.path = path;
        this.attributes = attributes;
        this.features = features;
    }

    public ShapefileEntry(String name, String owner, Date modificationDate, long size, String path) {
        super(name, owner, modificationDate, size);
        this.path = path;

    }

    public String getPath() {
        return path;
    }
    public void setPath(String path) {
        this.path = path;
    }

    public String[] getAttributes() { return attributes; }
    public void setAttributes(String[] attributes) { this.attributes = attributes; }

    public String[][] getFeatures() { return features; }
    public void setFeatures(String[][] features) { this.features = features; }

    @Override
    public String toString() {
        return "ShapefileEntry{name='" + getName() + "', owner='" + getOwner() + "', modificationDate='" + getModificationDate() + "', size='" + getSize() + "', path='" + path + "' #attributes=" + attributes.length + "}";
    }
}
