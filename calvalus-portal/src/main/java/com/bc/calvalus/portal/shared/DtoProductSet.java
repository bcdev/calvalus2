package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

import java.util.Date;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.inventory.ProductSet} class.
 *
 * @author Norman
 */
public class DtoProductSet implements IsSerializable {
    private String name;
    private String path;
    private Date minDate;
    private Date maxDate;


    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoProductSet() {
    }

    public DtoProductSet(String name, String path, Date minDate, Date maxDate) {
        this.name = name;
        this.path = path;
        this.minDate = minDate;
        this.maxDate = maxDate;
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    public Date getMinDate() {
        return minDate;
    }

    public Date getMaxDate() {
        return maxDate;
    }
}
