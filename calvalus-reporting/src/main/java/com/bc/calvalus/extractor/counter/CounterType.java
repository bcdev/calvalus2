
package com.bc.calvalus.extractor.counter;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.math.BigInteger;

/**
 * @author muhammad.bc.
 */

@XmlRootElement(name = "counter")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "counterType", propOrder = {
        "name",
        "totalCounterValue",
        "mapCounterValue",
        "reduceCounterValue"
})
public class CounterType {

    @XmlElement(required = true)
    private String name;
    @XmlElement(required = true)
    private BigInteger totalCounterValue;
    @XmlElement(required = true)
    private BigInteger mapCounterValue;
    @XmlElement(required = true)
    private String reduceCounterValue;

    /**
     * Gets the value of the name property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the value of the name property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setName(String value) {
        this.name = value;
    }

    /**
     * Gets the value of the totalCounterValue property.
     *
     * @return possible object is
     * {@link BigInteger }
     */
    public BigInteger getTotalCounterValue() {
        return totalCounterValue;
    }

}
