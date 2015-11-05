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

import com.bc.ceres.core.Assert;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

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

    public static final int NUM_ENTRIES = 7;
    public static final String DATE_PATTERN = "yyyy-MM-dd-HH-mm-ss";

    private final ProductData.UTC startTime;
    private final ProductData.UTC stopTime;
    private final int length;
    private final String message;

    private int processStartLine;
    private int processLength;
    private String productName;

    public static ProductInventoryEntry create(String startTime, String stopTime, String length, String processStartLine, String processLength, String message) throws ParseException {
        ProductData.UTC startTimeUTC = ProductData.UTC.parse(startTime, DATE_PATTERN);
        ProductData.UTC stopTimeUTC = ProductData.UTC.parse(stopTime, DATE_PATTERN);
        int lengthInt = Integer.parseInt(length);
        int processStartLineInt = Integer.parseInt(processStartLine);
        int processLengthInt = Integer.parseInt(processLength);
        return new ProductInventoryEntry(startTimeUTC, stopTimeUTC, lengthInt, processStartLineInt, processLengthInt, message);
    }

    public static ProductInventoryEntry createEmpty(String message) {
        return new ProductInventoryEntry(null, null, 0, 0, 0, message);
    }

    public static ProductInventoryEntry createForGoodProduct(Product product, String message) {
        return new ProductInventoryEntry(product.getStartTime(),
                                         product.getEndTime(),
                                         product.getSceneRasterHeight(),
                                         0,
                                         product.getSceneRasterHeight(),
                                         message);
    }

    public static ProductInventoryEntry createForBadProduct(Product product, String message) {
        return new ProductInventoryEntry(product.getStartTime(),
                                         product.getEndTime(),
                                         product.getSceneRasterHeight(),
                                         0,
                                         0,
                                         message);
    }


    private ProductInventoryEntry(ProductData.UTC startTime, ProductData.UTC stopTime, int length, int processStartLine, int processLength, String message) {
        this.startTime = startTime != null ? startTime : new ProductData.UTC();
        this.stopTime = stopTime != null ? stopTime : new ProductData.UTC();
        this.length = length;
        this.processStartLine = processStartLine;
        this.processLength = processLength;
        this.message = message;
    }

    public ProductData.UTC getStartTime() {
        return startTime;
    }

    public ProductData.UTC getStopTime() {
        return stopTime;
    }

    public int getLength() {
        return length;
    }

    public void setProcessStartLine(int processStartLine) {
        this.processStartLine = processStartLine;
    }

    public int getProcessStartLine() {
        return processStartLine;
    }

    public void setProcessLength(int processLength) {
        Assert.argument(processLength >= 0);
        this.processLength = processLength;
    }

    public int getProcessLength() {
        return processLength;
    }

    public String getMessage() {
        return message;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String toCSVString() {
        StringBuilder sb = new StringBuilder();
        sb.append(formatUTCDate(startTime));
        sb.append("\t");
        sb.append(formatUTCDate(stopTime));
        sb.append("\t");
        sb.append(length);
        sb.append("\t");
        sb.append(processStartLine);
        sb.append("\t");
        sb.append(processLength);
        sb.append("\t");        
        sb.append(message);
        return sb.toString();
    }

    private static String formatUTCDate(ProductData.UTC utc) {
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
