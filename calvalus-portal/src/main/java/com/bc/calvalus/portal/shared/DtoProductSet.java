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
    private Date minDate;
    private Date maxDate;


    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoProductSet() {
    }

    public DtoProductSet(String id, Date minDate, Date maxDate) {
        this.path = id;
        this.minDate = minDate;
        this.maxDate = maxDate;
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
