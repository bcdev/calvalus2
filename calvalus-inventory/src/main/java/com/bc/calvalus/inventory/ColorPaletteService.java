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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * The interface to the Calvalus color palette inventory.
 *
 * @author Declan
 */
public interface ColorPaletteService {

    /**
     * Gets color palette sets.
     *
     * @param filter A filter expression (unused)
     *
     * @return The array color palette sets, which may be empty.
     *
     * @throws java.io.IOException If an I/O error occurs
     */
    ColorPaletteSet[] getColorPaletteSets(String username, String filter) throws IOException;
}
