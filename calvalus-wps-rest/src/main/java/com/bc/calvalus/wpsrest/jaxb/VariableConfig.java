package com.bc.calvalus.wpsrest.jaxb;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Created by hans on 18/08/2015.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "VariableConfig", propOrder = {
            "name",
            "expr"
})
public class VariableConfig {

    @XmlElement(name = "name", nillable = false, required = true)
    protected String name;
    @XmlElement(name = "expr", nillable = false, required = true)
    protected String expr;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getExpr() {
        return expr;
    }

    public void setExpr(String expr) {
        this.expr = expr;
    }
}
