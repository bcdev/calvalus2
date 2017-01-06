
package com.bc.calvalus.generator.extractor.counter;

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
    protected String name;
    @XmlElement(required = true)
    protected BigInteger totalCounterValue;
    @XmlElement(required = true)
    protected BigInteger mapCounterValue;
    @XmlElement(required = true)
    protected String reduceCounterValue;

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

    /**
     * Sets the value of the totalCounterValue property.
     *
     * @param value allowed object is
     *              {@link BigInteger }
     */
    public void setTotalCounterValue(BigInteger value) {
        this.totalCounterValue = value;
    }

    /**
     * Gets the value of the mapCounterValue property.
     *
     * @return possible object is
     * {@link BigInteger }
     */
    public BigInteger getMapCounterValue() {
        return mapCounterValue;
    }

    /**
     * Sets the value of the mapCounterValue property.
     *
     * @param value allowed object is
     *              {@link BigInteger }
     */
    public void setMapCounterValue(BigInteger value) {
        this.mapCounterValue = value;
    }

    /**
     * Gets the value of the reduceCounterValue property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getReduceCounterValue() {
        return reduceCounterValue;
    }

    /**
     * Sets the value of the reduceCounterValue property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setReduceCounterValue(String value) {
        this.reduceCounterValue = value;
    }

}
