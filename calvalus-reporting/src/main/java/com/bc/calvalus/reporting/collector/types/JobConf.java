
package com.bc.calvalus.reporting.collector.types;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author muhammad.bc.
 * @author hans
 */

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
            "confXmlPath",
            "wpsJobId",
            "jobName",
            "remoteUser",
            "remoteRef",
            "processType",
            "workflowType",
            "dataProcessorUsed",
            "processorDescription",
            "mapClass",
            "inputPath",
            "inputType",
            "inputCollectionName",
            "outputDir",
            "outputType",
            "configuredCpuCores",
            "configuredRam",
            "systemName"
})
@XmlRootElement(name = "conf")
public class JobConf {

    @XmlElement(required = true)
    private String confXmlPath;
    @XmlElement(required = true)
    private String wpsJobId;
    @XmlElement(required = true)
    private String jobName;
    @XmlElement(required = true)
    private String remoteUser;
    @XmlElement(required = true)
    private String remoteRef;
    @XmlElement(required = true)
    private String processType;
    @XmlElement(required = true)
    private String workflowType;
    @XmlElement(required = true)
    private String dataProcessorUsed;
    @XmlElement(required = true)
    private String processorDescription;
    @XmlElement(required = true)
    private String mapClass;
    @XmlElement(required = true)
    private String inputPath;
    @XmlElement(required = true)
    private String inputType;
    @XmlElement(required = true)
    private String inputCollectionName;
    @XmlElement(required = true)
    private String outputDir;
    @XmlElement(required = true)
    private String outputType;
    @XmlElement(required = true)
    private String configuredCpuCores;
    @XmlElement(required = true)
    private String configuredRam;
    @XmlElement(required = true)
    private String systemName;

    public String getConfXmlPath() {
        return confXmlPath;
    }

    public void setConfXmlPath(String value) {
        this.confXmlPath = value;
    }

    public String getWpsJobId() {
        return wpsJobId;
    }

    public void setWpsJobId(String wpsJobId) {
        this.wpsJobId = wpsJobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public void setRemoteUser(String remoteUser) {
        this.remoteUser = remoteUser;
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

    public String getWorkflowType() {
        return workflowType;
    }

    public void setWorkflowType(String workflowType) {
        this.workflowType = workflowType;
    }

    public String getDataProcessorUsed() {
        return dataProcessorUsed;
    }

    public void setDataProcessorUsed(String dataProcessorUsed) {
        this.dataProcessorUsed = dataProcessorUsed;
    }

    public String getProcessorDescription() {
        return processorDescription;
    }

    public void setProcessorDescription(String processorDescription) {
        this.processorDescription = processorDescription;
    }

    public String getMapClass() {
        return mapClass;
    }

    public void setMapClass(String mapClass) {
        this.mapClass = mapClass;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getInputType() {
        return inputType;
    }

    public void setInputType(String inputType) {
        this.inputType = inputType;
    }

    public String getInputCollectionName() {
        return inputCollectionName;
    }

    public void setInputCollectionName(String inputCollectionName) {
        this.inputCollectionName = inputCollectionName;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    public String getOutputType() {
        return outputType;
    }

    public void setOutputType(String outputType) {
        this.outputType = outputType;
    }

    public String getConfiguredCpuCores() {
        return configuredCpuCores;
    }

    public void setConfiguredCpuCores(String configuredCpuCores) {
        this.configuredCpuCores = configuredCpuCores;
    }

    public String getConfiguredRam() {
        return configuredRam;
    }

    public void setConfiguredRam(String configuredRam) {
        this.configuredRam = configuredRam;
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }
}
