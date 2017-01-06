
package com.bc.calvalus.generator.log.counter;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author muhammad.bc.
 */

@XmlRootElement(name = "jobCounters")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "jobCountersType", propOrder = {
        "id",
        "counterGroup"
})
public class CountersType {

    @XmlElement(required = true)
    protected String id;
    @XmlElement(required = true)
    protected CounterGroupType counterGroup;

    /**
     * Gets the value of the id property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the value of the id property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setId(String value) {
        this.id = value;
    }

    /**
     * Gets the value of the counterGroup property.
     *
     * @return possible object is
     * {@link CounterGroupType }
     */
    public CounterGroupType getCounterGroup() {
        return counterGroup;
    }

    /**
     * Sets the value of the counterGroup property.
     *
     * @param value allowed object is
     *              {@link CounterGroupType }
     */
    public void setCounterGroup(CounterGroupType value) {
        this.counterGroup = value;
    }

}
