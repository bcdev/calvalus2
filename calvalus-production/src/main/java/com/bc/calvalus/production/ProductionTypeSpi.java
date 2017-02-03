package com.bc.calvalus.production;

import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.staging.StagingService;

/**
 * Abstraction of a product type service provider interface (workflow implementation).
 *
 * @author MarcoZ
 */
public interface ProductionTypeSpi {
    ProductionType create(FileSystemService fileSystemService, ProcessingService processing, StagingService staging);
}
