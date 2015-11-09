package com.bc.calvalus.production;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.staging.StagingService;

/**
 * Abstraction of a product type service exceptionmapper interface (workflow implementation).
 *
 * @author MarcoZ
 */
public interface ProductionTypeSpi {
    ProductionType create(InventoryService inventory, ProcessingService processing, StagingService staging);
}
