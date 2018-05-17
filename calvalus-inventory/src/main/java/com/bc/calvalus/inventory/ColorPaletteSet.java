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
 * Color palette set.
 *
 * @author Declan
 */
public class ColorPaletteSet {

    private final String name;
    private final String path;

    public ColorPaletteSet(String name, String path) {
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (path == null) {
            throw new NullPointerException("path");
        }
        this.name = name;
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public String getPath() { return path; }
}
