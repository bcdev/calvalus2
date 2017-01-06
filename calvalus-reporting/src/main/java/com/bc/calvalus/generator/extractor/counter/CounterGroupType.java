
package com.bc.calvalus.generator.extractor.counter;

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
public class CounterGroupType {

    @XmlElement(required = true)
    protected String counterGroupName;
    protected List<CounterType> counter;

    /**
     * Gets the value of the counterGroupName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCounterGroupName() {
        return counterGroupName;
    }

    /**
     * Sets the value of the counterGroupName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCounterGroupName(String value) {
        this.counterGroupName = value;
    }

    /**
     * Gets the value of the counter property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the counter property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getCounter().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link CounterType }
     * 
     * 
     */
    public List<CounterType> getCounter() {
        if (counter == null) {
            counter = new ArrayList<CounterType>();
        }
        return this.counter;
    }

}
