package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

import java.util.Date;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.catalogue.ProductSet} class.
 *
 * @author Norman
 */
public class DtoProductSet implements IsSerializable {
    private String path;
    private String type;
    private String name;
    private Date minDate;
    private Date maxDate;


    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoProductSet() {
        this.path = null;
        this.type = "";
        this.name = "";
    }

    public DtoProductSet(String id, String type, String name, Date minDate, Date maxDate) {
        this.path = id;
        this.type = type;
        this.name = name;
        this.minDate = minDate;
        this.maxDate = maxDate;
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

    public Date getMinDate() {
        return minDate;
    }

    public Date getMaxDate() {
        return maxDate;
    }
}
