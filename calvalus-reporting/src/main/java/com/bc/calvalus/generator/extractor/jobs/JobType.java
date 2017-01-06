
package com.bc.calvalus.generator.extractor.jobs;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.datatype.XMLGregorianCalendar;

/**
 * @author muhammad.bc.
 */

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "job.xml")
@XmlType(name = "jobType", propOrder = {
        "submitTime",
        "startTime",
        "finishTime",
        "id",
        "name",
        "queue",
        "user",
        "state",
        "mapsTotal",
        "mapsCompleted",
        "reducesTotal",
        "reducesCompleted"
})
public class JobType {

    @XmlElement(required = true)
    protected String submitTime;
    @XmlElement(required = true)
    protected String startTime;
    @XmlElement(required = true)
    protected String finishTime;
    @XmlElement(required = true)
    protected String id;
    @XmlElement(required = true)
    protected String name;
    @XmlElement(required = true)
    protected String queue;
    @XmlElement(required = true)
    protected String user;
    @XmlElement(required = true)
    protected String state;
    @XmlElement(required = true)
    protected String mapsTotal;
    @XmlElement(required = true)
    protected String mapsCompleted;
    @XmlElement(required = true)
    protected String reducesTotal;
    @XmlElement(required = true)
    protected String reducesCompleted;


    public String getSubmitTime() {
        return submitTime;
    }


    public void setSubmitTime(String value) {
        this.submitTime = value;
    }

    public String getStartTime() {
        return startTime;
    }


    public void setStartTime(String value) {
        this.startTime = value;
    }

    /**
     * Gets the value of the finishTime property.
     *
     * @return possible object is
     * {@link XMLGregorianCalendar }
     */
    public String getFinishTime() {
        return finishTime;
    }

    /**
     * Sets the value of the finishTime property.
     *
     * @param value allowed object is
     *              {@link XMLGregorianCalendar }
     */
    public void setFinishTime(String value) {
        this.finishTime = value;
    }

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
     * Gets the value of the queue property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getQueue() {
        return queue;
    }

    /**
     * Sets the value of the queue property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setQueue(String value) {
        this.queue = value;
    }

    /**
     * Gets the value of the user property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getUser() {
        return user;
    }

    /**
     * Sets the value of the user property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setUser(String value) {
        this.user = value;
    }

    /**
     * Gets the value of the state property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getState() {
        return state;
    }

    /**
     * Sets the value of the state property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setState(String value) {
        this.state = value;
    }

    /**
     * Gets the value of the mapsTotal property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getMapsTotal() {
        return mapsTotal;
    }

    /**
     * Sets the value of the mapsTotal property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setMapsTotal(String value) {
        this.mapsTotal = value;
    }

    /**
     * Gets the value of the mapsCompleted property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getMapsCompleted() {
        return mapsCompleted;
    }

    /**
     * Sets the value of the mapsCompleted property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setMapsCompleted(String value) {
        this.mapsCompleted = value;
    }

    /**
     * Gets the value of the reducesTotal property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getReducesTotal() {
        return reducesTotal;
    }

    /**
     * Sets the value of the reducesTotal property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setReducesTotal(String value) {
        this.reducesTotal = value;
    }

    /**
     * Gets the value of the reducesCompleted property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getReducesCompleted() {
        return reducesCompleted;
    }

    /**
     * Sets the value of the reducesCompleted property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setReducesCompleted(String value) {
        this.reducesCompleted = value;
    }

}
