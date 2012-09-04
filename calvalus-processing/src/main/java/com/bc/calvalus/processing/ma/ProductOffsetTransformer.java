/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de) 
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

package com.bc.calvalus.processing.ma;

/**
 * Shifts the "pixel_x" and "pixel_Y" values by the given offsets.
 */
public class ProductOffsetTransformer implements RecordTransformer {

    private final int xAttributeIndex;
    private final int yAttributeIndex;
    private final int offsetX;
    private final int offsetY;

    public ProductOffsetTransformer(int xAttributeIndex, int yAttributeIndex, int offsetX, int offsetY) {
        this.xAttributeIndex = xAttributeIndex;
        this.yAttributeIndex = yAttributeIndex;
        this.offsetX = offsetX;
        this.offsetY = offsetY;
    }

    @Override
    public Record transform(Record record) {
        if (offsetX == 0 && offsetY == 0) {
            return record;
        }
        Object[] attributeValues = record.getAttributeValues();
        for (int i = 0; i < attributeValues.length; i++) {
            Object attributeValue = attributeValues[i];
            if (i == xAttributeIndex && attributeValue instanceof int[]) {
                int[] xValues = (int[]) attributeValue;
                for (int j = 0; j < xValues.length; j++) {
                    xValues[j] = xValues[j] + offsetX;
                }
            } else if (i == yAttributeIndex && attributeValue instanceof int[]) {
                int[] yValues = (int[]) attributeValue;
                for (int j = 0; j < yValues.length; j++) {
                    yValues[j] = yValues[j] + offsetY;
                }
            }
        }
        return record;
    }
}
