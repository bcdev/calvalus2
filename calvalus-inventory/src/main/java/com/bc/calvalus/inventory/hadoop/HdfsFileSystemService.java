/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.inventory.hadoop;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.inventory.AbstractFileSystemService;

/**
 * An filesystem service that uses Hadoop's HDFS.
 *
 * @author MarcoZ
 */
public class HdfsFileSystemService extends AbstractFileSystemService {

    public static final String CONTEXT_PATH = "/calvalus";

    public HdfsFileSystemService(JobClientsMap jobClientsMap) {
        super(jobClientsMap);
    }

    @Override
    protected String getContextPath() {
        return CONTEXT_PATH;
    }
}
