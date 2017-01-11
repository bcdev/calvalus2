
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
})
@XmlRootElement(name = "conf")
public class Conf {

    @XmlElement(required = true)
    private String path;

    public String getPath() {
        return path;
    }

    public void setPath(String value) {
        this.path = value;
    }
}
