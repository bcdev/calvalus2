/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.inventory;

import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.StringUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * this flass is responsible for converting a {@link ProductSet} into a {@link String} and back.
 *
 * @author MarcoZ
 */
public class ProductSetPersistable {

    private static final String DATE_PATTERN = "yyyy-MM-dd";
    private static final DateFormat DATE_FORMAT = ProductData.UTC.createDateFormat(DATE_PATTERN);
    public static final String FILENAME = "product-sets.csv";

    private ProductSetPersistable() {
    }

    public static String convertToCSV(ProductSet productSet) {
        StringBuilder sb = new StringBuilder();
        sb.append(productSet.getProductType());
        sb.append(';');
        sb.append(productSet.getName());
        sb.append(';');
        sb.append(productSet.getPath());
        sb.append(';');
        sb.append(format(productSet.getMinDate()));
        sb.append(';');
        sb.append(format(productSet.getMaxDate()));
        sb.append(';');
        sb.append(productSet.getRegionName());
        sb.append(';');
        sb.append(productSet.getRegionWKT());
        sb.append(';');
        sb.append(StringUtils.join(productSet.getBandNames(), ","));
        return sb.toString();
    }

    public static ProductSet convertFromCSV(String text) {
        String trimmedText = text.trim();
        if (trimmedText.startsWith("#") || trimmedText.isEmpty()) {
            return null; //comments are ignored
        }
        String[] splits = trimmedText.split(";");
        if (splits.length == 7) {
            String productType = nullAware(splits[0]);
            String name = nullAware(splits[1]);
            String path = nullAware(splits[2]);
            Date date1 = asDate(splits[3]);
            Date date2 = asDate(splits[4]);
            String regionName = nullAware(splits[5]);
            String regionWKT = nullAware(splits[6]);
            return new ProductSet(productType, name, path, date1, date2, regionName, regionWKT,  new String[0]);
        } else if (splits.length == 8) {
            String productType = nullAware(splits[0]);
            String name = nullAware(splits[1]);
            String path = nullAware(splits[2]);
            Date date1 = asDate(splits[3]);
            Date date2 = asDate(splits[4]);
            String regionName = nullAware(splits[5]);
            String regionWKT = nullAware(splits[6]);
            String bandNameCSV = nullAware(splits[7]);
            String[] bandNames =  new String[0];
            if (bandNameCSV != null) {
                bandNames = bandNameCSV.split(",");
            }
            return new ProductSet(productType, name, path, date1, date2, regionName, regionWKT, bandNames);
        }
        return null;
    }

    static String nullAware(String text) {
        if (text == null || "null".equals(text)) {
            return null;
        } else {
            return text;
        }
    }

    static Date asDate(String text) {
        try {
            return DATE_FORMAT.parse(text);
        } catch (ParseException e) {
            return null;
        }
    }

    static String format(Date date) {
        if (date == null) {
            return "null";
        } else {
            return DATE_FORMAT.format(date);
        }
    }

}
