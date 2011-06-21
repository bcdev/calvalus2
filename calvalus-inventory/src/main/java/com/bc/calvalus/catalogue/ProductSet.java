package com.bc.calvalus.catalogue;


/**
 * A product set.
 *
 * @author Norman
 */
public class ProductSet {
    private String path;
    private String type;
    private String name;

    public ProductSet(String path, String type, String name) {
        this.path = path;
        this.type = type;
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }
}
