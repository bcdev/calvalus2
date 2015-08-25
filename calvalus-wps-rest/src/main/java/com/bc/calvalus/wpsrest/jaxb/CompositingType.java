package com.bc.calvalus.wpsrest.jaxb;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;

/**
 * Created by hans on 18/08/2015.
 */

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CompositingType", propOrder = {
            "value"
})
public enum CompositingType {
    BINNING,
    MOSAICKING;

    CompositingType() {
    }
}
