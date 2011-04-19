/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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

/**
 * Names of Hadoop job configuration parameters.
 */
public interface JobConfNames {
    public static final String CALVALUS_IDENTIFIER = "calvalus.identifier";
    public static final String CALVALUS_BUNDLE = "calvalus.bundle";
    public static final String CALVALUS_INPUT = "calvalus.input";
    public static final String CALVALUS_OUTPUT = "calvalus.output";
    public static final String CALVALUS_L2_OPERATOR = "calvalus.l2.operator";
    public static final String CALVALUS_L2_PARAMETERS = "calvalus.l2.parameters";
    public static final String CALVALUS_L3_PARAMETERS = "calvalus.l3.parameters";
    public static final String CALVALUS_TA_PARAMETERS = "calvalus.ta.parameters";
    public static final String CALVALUS_FORMATTER_PARAMETERS = "calvalus.formatter.parameters";
    public static final String CALVALUS_SYSTEM_PROPERTIES = "calvalus.system.properties";
    public static final String CALVALUS_START_DATE = "calvalus.startDate";
    public static final String CALVALUS_STOP_DATE = "calvalus.stopDate";
    public static final String CALVALUS_REGION_GEOMETRY = "calvalus.regionGeometry";
}
