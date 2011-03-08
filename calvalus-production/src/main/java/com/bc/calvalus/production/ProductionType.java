package com.bc.calvalus.production;

import com.bc.calvalus.staging.Staging;

/**
 * Abstraction of a product type (workflow implementation).
 *
 * @author MarcoZ
 * @author Norman
 */
public interface ProductionType {
    String getName();

    Production createProduction(ProductionRequest productionRequest) throws ProductionException;

    Staging createStaging(Production production) throws ProductionException;
}
