
package com.bc.calvalus.reporting.collector.types;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * @author muhammad.bc.
 */

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "jobs")
@XmlType(name = "jobsType", propOrder = {"job"})
public class Jobs {

    private List<Job> job;

    public List<Job> getJob() {
        if (job == null) {
            job = new ArrayList<>();
        }
        return this.job;
    }
}
