/*
 * Copyright (C) 2018 Brockmann Consult GmbH (info@brockmann-consult.de)
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

/**
 * this class is responsible for converting a {@link ProductSet} into a {@link String} and back.
 *
 * @author Declan
 */
public class ColorPaletteSetPersistable {

    public static final String FILENAME = "color-palette-sets.csv";

    private ColorPaletteSetPersistable() {
    }

    public static String convertToCSV(ColorPaletteSet colorPaletteSet) {
        StringBuilder sb = new StringBuilder();
        sb.append(colorPaletteSet.getName());
        sb.append(';');
        sb.append(colorPaletteSet.getPath());
        sb.append(';');
        return sb.toString();
    }

    public static ColorPaletteSet convertFromCSV(String text) {
        String trimmedText = text.trim();
        if (trimmedText.startsWith("#") || trimmedText.isEmpty()) {
            return null; //comments are ignored
        }
        String[] splits = trimmedText.split(";");

        String name;
        String path;

        if (splits.length >= 2) {
            name = nullAware(splits[0]);
            path = nullAware(splits[1]);
        } else {
            // less than 2 fields currently not supported
            return null;
        }
        return new ColorPaletteSet(name, path);
    }

    static String nullAware(String text) {
        if (text == null || "null".equals(text)) {
            return null;
        } else {
            return text;
        }
    }
}
