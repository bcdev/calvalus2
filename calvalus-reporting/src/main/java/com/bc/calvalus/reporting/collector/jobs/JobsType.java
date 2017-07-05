
package com.bc.calvalus.reporting.collector.jobs;

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
@XmlType(name = "jobsType", propOrder = {"jobs"})
public class JobsType {

    private List<JobType> jobs;

    public List<JobType> getJobs() {
        if (jobs == null) {
            jobs = new ArrayList<>();
        }
        return this.jobs;
    }
}
