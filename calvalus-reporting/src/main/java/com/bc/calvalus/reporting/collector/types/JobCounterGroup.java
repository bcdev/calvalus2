
package com.bc.calvalus.reporting.collector.types;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * @author muhammad.bc.
 */

@XmlRootElement(name = "counterGroup")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "counterGroupType", propOrder = {
    "counterGroupName",
    "counter"
})
public class JobCounterGroup {

    @XmlElement(required = true)
    private String counterGroupName;
    private List<JobCounter> counter;


    public String getCounterGroupName() {
        return counterGroupName;
    }

    public void setCounterGroupName(String value) {
        this.counterGroupName = value;
    }

    public List<JobCounter> getCounter() {
        if (counter == null) {
            counter = new ArrayList<>();
        }
        return this.counter;
    }

}
