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

package com.bc.calvalus.production.hadoop;

import org.esa.beam.framework.datamodel.ProductData;

import java.util.*;

/**
 * Transforms input path patterns into input path globs.
 * The pattern may be any path glob which may currently contain the variable references:
 * <ol>
 * <li>{@code ${region}} - the region name </li>
 * <li>{@code ${yyyy}} - the (sensing) year of a product file</li>
 * <li>{@code ${MM}} - the (sensing) month of a product file</li>
 * <li>{@code ${dd}} - the (sensing) day of a product file</li>
 * </ol>
 *
 * @author MarcoZ
 * @author Norman
 */
public class InputPathResolver {

    String region;
    Date minDate;
    Date maxDate;

    public InputPathResolver() {
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public Date getMinDate() {
        return minDate;
    }

    public void setMinDate(Date minDate) {
        this.minDate = minDate;
    }

    public Date getMaxDate() {
        return maxDate;
    }

    public void setMaxDate(Date maxDate) {
        this.maxDate = maxDate;
    }

    public List<String> resolve(String inputPathPattern) {
        if (region != null) {
            inputPathPattern = inputPathPattern.replace("${region}", region);
        } else {
            inputPathPattern = inputPathPattern.replace("${region}", "*");
        }
        List<String> globList = new ArrayList<String>(128);

        if (minDate != null && maxDate != null) {
            Set<String> globSet = new HashSet<String>(517);
            Calendar calendar = ProductData.UTC.createCalendar();
            calendar.setTime(minDate);
            Calendar stopCal = ProductData.UTC.createCalendar();
            stopCal.setTime(maxDate);
            do {
                String glob = inputPathPattern.replace("${yyyy}", String.format("%tY", calendar));
                glob = glob.replace("${MM}", String.format("%tm", calendar));
                glob = glob.replace("${dd}", String.format("%td", calendar));
                if (!globSet.contains(glob)) {
                    globSet.add(glob);
                    globList.add(glob);
                }
                calendar.add(Calendar.DAY_OF_WEEK, 1);
            } while (!calendar.after(stopCal));
        } else {
            String glob = inputPathPattern.replace("${yyyy}", "*");
            glob = glob.replace("${MM}", "*");
            glob = glob.replace("${dd}", "*");
            globList.add(glob);
        }
        return globList;
    }

    public static List<String> getInputPathGlobs(String inputPathPattern, String region, Date minDate, Date maxDate) {
        InputPathResolver inputPathResolver = new InputPathResolver();
        inputPathResolver.setRegion(region);
        inputPathResolver.setMinDate(minDate);
        inputPathResolver.setMaxDate(maxDate);
        return inputPathResolver.resolve(inputPathPattern);
    }
}
