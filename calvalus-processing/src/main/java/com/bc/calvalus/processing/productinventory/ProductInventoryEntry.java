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

package com.bc.calvalus.processing.productinventory;

import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

/**
 * An entry for the {@link ProductInventory}.
 *
 * @author MarcoZ
 */
public class ProductInventoryEntry {

    public static final String DATE_PATTERN = "yyyy-MM-dd-HH-mm-ss";

    public static ProductInventoryEntry create(String startTime, String stopTime, String startLine, String stopLine, String message) throws ParseException {
        ProductData.UTC startTimeUTC = ProductData.UTC.parse(startTime, DATE_PATTERN);
        ProductData.UTC stopTimeUTC = ProductData.UTC.parse(stopTime, DATE_PATTERN);
        int startLineInt = Integer.parseInt(startLine);
        int stopLineInt = Integer.parseInt(stopLine);
        return new ProductInventoryEntry(startTimeUTC, stopTimeUTC, startLineInt, stopLineInt, message);
    }

    public static ProductInventoryEntry createEmpty(String message) {
        return new ProductInventoryEntry(null, null, -1, -1, message);
    }

    public static ProductInventoryEntry createForProduct(Product product, String message) {
        return new ProductInventoryEntry(product.getStartTime(),
                                         product.getEndTime(),
                                         0,
                                         product.getSceneRasterHeight() - 1,
                                         message);
    }

    final ProductData.UTC startTime;
    final ProductData.UTC stopTime;
    final int startLine;
    final int stopLine;
    final String message;

    private ProductInventoryEntry(ProductData.UTC startTime, ProductData.UTC stopTime, int startLine, int stopLine, String message) {
        this.startTime = startTime;
        this.stopTime = stopTime;
        this.startLine = startLine;
        this.stopLine = stopLine;
        this.message = message;
    }

    public ProductData.UTC getStartTime() {
        return startTime;
    }

    public ProductData.UTC getStopTime() {
        return stopTime;
    }

    public int getStartLine() {
        return startLine;
    }

    public int getStopLine() {
        return stopLine;
    }

    public String getMessage() {
        return message;
    }

    public String toCSVString() {
        StringBuilder sb = new StringBuilder();
        appendDate(sb, startTime);
        sb.append("\t");
        appendDate(sb, stopTime);
        sb.append("\t");
        sb.append(startLine);
        sb.append("\t");
        sb.append(stopLine);
        sb.append("\t");
        sb.append(message);
        return sb.toString();
    }

    private static void appendDate(StringBuilder sb, ProductData.UTC utc) {
        if (utc != null) {
            sb.append(format(utc));
        } else {
            sb.append("null");
        }
    }

    private static String format(ProductData.UTC utc) {
        final Calendar calendar = ProductData.UTC.createCalendar();
        calendar.add(Calendar.DATE, utc.getDaysFraction());
        calendar.add(Calendar.SECOND, (int) utc.getSecondsFraction());
        final DateFormat dateFormat = ProductData.UTC.createDateFormat(DATE_PATTERN);
        final Date time = calendar.getTime();
        final String dateString = dateFormat.format(time);
        final String microsString = String.valueOf(utc.getMicroSecondsFraction());
        StringBuffer sb = new StringBuffer(dateString.toUpperCase());
        sb.append('.');
        for (int i = microsString.length(); i < 6; i++) {
            sb.append('0');
        }
        sb.append(microsString);
        return sb.toString();
    }


}
