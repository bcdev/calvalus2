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

package com.bc.calvalus.production;

import com.bc.calvalus.inventory.ColorPaletteService;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.inventory.InventoryService;
import org.apache.hadoop.conf.Configuration;

/**
 * Hold instances of all services in use.
 */
public class ServiceContainer {
    private final ProductionService productionService;
    private final FileSystemService fileSystemService;
    private final InventoryService inventoryService;
    private final ColorPaletteService colorPaletteService;
    private final Configuration hadoopConfiguration;

    public ServiceContainer(ProductionService productionService, 
                            FileSystemService fileSystemService, 
                            InventoryService inventoryService,
                            ColorPaletteService colorPaletteService,
                            Configuration hadoopConfiguration) {
        this.productionService = productionService;
        this.fileSystemService = fileSystemService;
        this.inventoryService = inventoryService;
        this.colorPaletteService = colorPaletteService;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    public ProductionService getProductionService() {
        return productionService;
    }

    public FileSystemService getFileSystemService() {
        return fileSystemService;
    }

    public InventoryService getInventoryService() {
        return inventoryService;
    }

    public ColorPaletteService getColorPaletteService() {
        return colorPaletteService;
    }

    public Configuration getHadoopConfiguration() {
        return hadoopConfiguration;
    }

    public void close() throws ProductionException {
        if (productionService != null) {
            productionService.close();
        }
    }
}
