
package com.bc.calvalus.generator.extractor.configuration;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author muhammad.bc.
 */

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
        "path",
        "wpsJobId",
        "remoteUser",
        "jobName",
        "remoteRef",
        "processType",

})


@XmlRootElement(name = "conf")
public class Conf {

    @XmlElement(required = true)
    private String path;
    @XmlElement(required = true)
    private String wpsJobId;
    @XmlElement(required = true)
    private String remoteUser;
    @XmlElement(required = true)
    private String jobName;
    @XmlElement(required = true)
    private String remoteRef;
    @XmlElement(required = true)
    private String processType;

    public String getWpsJobId() {
        return wpsJobId;
    }

    public void setWpsJobId(String wpsJobId) {
        this.wpsJobId = wpsJobId;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public void setRemoteUser(String remoteUser) {
        this.remoteUser = remoteUser;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getRemoteRef() {
        return remoteRef;
    }

    public void setRemoteRef(String remoteRef) {
        this.remoteRef = remoteRef;
    }

    public String getProcessType() {
        return processType;
    }

    public void setProcessType(String processType) {
        this.processType = processType;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String value) {
        this.path = value;
    }

}
