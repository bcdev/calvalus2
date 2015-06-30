/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing;

import com.bc.ceres.core.Assert;
import org.esa.snap.framework.gpf.annotations.Parameter;

import java.util.HashMap;
import java.util.Map;

public class AggregatorDescriptor {

    @Parameter
    private String aggregator;

    @Parameter
    private String variable;
    @Parameter
    private String parameter;
    // Short description in XHTML
    @Parameter
    private String descriptionHtml;

    @Parameter(itemAlias = "jobParameter")
    private JobParameter[] jobConfig;

    // empty constructor for XML serialization
    public AggregatorDescriptor() {
    }

    public AggregatorDescriptor(String aggregator,
                                String variable,
                                String parameter) {

        Assert.notNull(aggregator, "aggregator");
        Assert.notNull(variable, "variable");
        this.aggregator = aggregator;
        this.variable = variable;
        this.parameter = parameter;
    }

    public String getAggregator() {
        return aggregator;
    }

    public String getVariable() {
        return variable;
    }

    public String getParameter() {
        return parameter;
    }

    public String getDescriptionHtml() {
        return descriptionHtml;
    }

    public Map<String, String> getJobConfiguration() {
        HashMap<String, String> map = new HashMap<String, String>();
        if (jobConfig != null) {
            for (JobParameter jobParameter : jobConfig) {
                map.put(jobParameter.name, jobParameter.value);
            }
        }
        return map;
    }

    public static class JobParameter {

        private String name;
        private String value;
    }

}
