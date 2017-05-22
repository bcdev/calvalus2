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

package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * Details about a shapefile.
 * 
 * @author marcoz 
 */
public class DtoRegionDataInfo implements IsSerializable {

    private String[] header;
    private String[][] values;

    // required for the framework
    public DtoRegionDataInfo() {
    }

    public DtoRegionDataInfo(String[] header, String[][] values) {
        this.header = header;
        this.values = values;
    }

    public String[] getHeader() {
        return header;
    }

    public String[][] getValues() {
        return values;
    }
    
    public String[] getValues(int col) {
        String[] colValues = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            colValues[i] = values[i][col];
        }
        return colValues;
    }
    
}
