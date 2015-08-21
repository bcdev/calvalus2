package com.bc.calvalus.wps2.jaxb;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import java.util.List;

/**
 * Created by hans on 20/08/2015.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "aggregators", propOrder = {
            "aggregator"
})
public class Aggregators {

    @XmlElement(name = "aggregator", nillable = false, required = true)
    protected List<AggregatorConfig> aggregator;

    public List<AggregatorConfig> getAggregator() {
        return aggregator;
    }
}
