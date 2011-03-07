package com.bc.calvalus.production;

/**
 * Abstraction of a product type (workflow implementation).
 *
 * @author MarcoZ
 * @author Norman
 */
public interface ProductionType {
    String getName();

    Production orderProduction(ProductionRequest productionRequest) throws ProductionException;

    Staging stageProduction(Production production) throws ProductionException;
}
