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

package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * @author Declan
 */
public class DtoColorPalette implements IsSerializable {

    private String name;
    private String[] path;
    private String cpdURL;

    /**
     * No-arg constructor as required by {@link IsSerializable}. Don't use directly.
     */
    public DtoColorPalette() {
    }

    public DtoColorPalette(String name, String[] path, String cpdURL) {
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (path == null) {
            throw new NullPointerException("path");
        }
        if (path == null) {
            throw new NullPointerException("cpdURL");
        }
        this.name = name;
        this.path = path;
        this.cpdURL = cpdURL;
    }

    public String getName() {
        return name;
    }

    public String[] getPath() {
        return path;
    }

    public String getCpdURL() {
        return cpdURL;
    }

    public boolean isUserColorPalette() {
        return path != null && path.length > 0 && "user".equals(path[0]);
    }

    public String getQualifiedName() {
        StringBuilder sb = new StringBuilder();
        for (String pathElement : path) {
            sb.append(pathElement);
            sb.append(".");
        }
        sb.append(name);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DtoColorPalette that = (DtoColorPalette) o;

        if (!name.equals(that.name)) {
            return false;
        }
        if (!path.equals(that.path)) {
            return false;
        }
        if (!path.equals(that.cpdURL)) {
            return false;
        }
        return true;
    }
}
