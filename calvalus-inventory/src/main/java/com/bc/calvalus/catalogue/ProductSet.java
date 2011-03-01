package com.bc.calvalus.catalogue;


/**
 * A product set.
 *
 * @author Norman
 */
public class ProductSet {
    private String id;
    private String type;
    private String name;

    public ProductSet(String id, String type, String name) {
        this.id = id;
        this.type = type;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }
}
