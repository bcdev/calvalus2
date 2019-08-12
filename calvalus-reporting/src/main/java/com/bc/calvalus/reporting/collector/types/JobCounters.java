
package com.bc.calvalus.reporting.collector.types;

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
public class JobCounters {

    @XmlElement(required = true)
    private String id;
    @XmlElement(required = true)
    private JobCounterGroup counterGroup;

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
     * {@link JobCounterGroup }
     */
    public JobCounterGroup getCounterGroup() {
        return counterGroup;
    }

    /**
     * Sets the value of the counterGroup property.
     *
     * @param value allowed object is
     *              {@link JobCounterGroup }
     */
    public void setCounterGroup(JobCounterGroup value) {
        this.counterGroup = value;
    }

}
