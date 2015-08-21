package com.bc.calvalus.wps2.jaxb;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;

/**
 * Created by hans on 18/08/2015.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "TimeFilterMethod", propOrder = {
            "value"
})
public enum TimeFilterMethod {
    NONE,
    TIME_RANGE,
    SPATIOTEMPORAL_DATA_DAY;

    TimeFilterMethod() {
    }

}
