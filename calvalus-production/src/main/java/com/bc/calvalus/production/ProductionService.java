package com.bc.calvalus.production;

/**
 * The interface to the Calvalus production service.
 *
 * @author Norman
 */
public interface ProductionService {
    /**
     * Commissions a production.
     *
     * @param request The production request.
     * @return The production response.
     */
    ProductionResponse produce(ProductionRequest request);
}
