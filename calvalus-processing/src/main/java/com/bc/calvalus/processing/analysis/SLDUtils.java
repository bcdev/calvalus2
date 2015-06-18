/*
 * Copyright (C) 2015 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.analysis;

import org.geotools.factory.CommonFactoryFinder;
import org.geotools.styling.SLDParser;
import org.geotools.styling.Style;
import org.geotools.styling.StyleFactory;

import java.io.File;
import java.io.IOException;

public class SLDUtils {
    private static final StyleFactory styleFactory = CommonFactoryFinder.getStyleFactory(null);

    public static File getSLDFile(File shapeFile) {
        String filename = shapeFile.getAbsolutePath();
        if (filename.endsWith(".shp") || filename.endsWith(".dbf")
                || filename.endsWith(".shx")) {
            filename = filename.substring(0, filename.length() - 4);
            filename += ".sld";
        } else if (filename.endsWith(".SHP") || filename.endsWith(".DBF")
                || filename.endsWith(".SHX")) {
            filename = filename.substring(0, filename.length() - 4);
            filename += ".SLD";
        }
        return new File(filename);
    }

    public static Style[] loadSLD(File shapeFile) {
        File sld = getSLDFile(shapeFile);
        if (sld.exists()) {
            return createFromSLD(sld);
        } else {
            return new Style[0];
        }
    }

    public static Style[] createFromSLD(File sld) {
        try {
            SLDParser stylereader = new SLDParser(styleFactory, sld.toURI().toURL());
            return stylereader.readXML();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new Style[0];
    }
}