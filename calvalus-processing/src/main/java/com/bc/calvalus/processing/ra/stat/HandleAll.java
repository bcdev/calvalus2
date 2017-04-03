/*
 * Copyright (C) 2017 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ra.stat;

/**
 * Implements the logic that ensures that all elements of an sequence (starting with 0) are handled.
 */
class HandleAll {

    private final int numItems;
    private int currentItem;

    HandleAll(int numItems) {
        this.numItems = numItems;
        this.currentItem = -1;
    }

    int current() {
        return currentItem;
    }

    int[] next(int nextIndex) {
        if (nextIndex <= currentItem) {
            String msg = String.format("nextIndex(%d) <= currentItem(%d)", nextIndex, currentItem);
            throw new IllegalArgumentException(msg);
        }
        if (nextIndex >= numItems) {
            String msg = String.format("nextIndex(%d) >= numItems(%d)", nextIndex, numItems);
            throw new IllegalArgumentException(msg);
        }
        int[] indices = getIndices(currentItem, nextIndex);
        currentItem = nextIndex;
        return indices;
    }

    int[] remaining() {
        return getIndices(currentItem, numItems);
    }

    void reset() {
        currentItem = -1;
    }

    private int[] getIndices(int beginIndex, int endIndex) {
        int[] indices = new int[endIndex - beginIndex - 1];
        for (int itemI = beginIndex + 1, i = 0; itemI < endIndex; itemI++, i++) {
            indices[i] = itemI;
        }
        return indices;
    }
}
