package org.esa.beam.gpf.operators.matchup;

import org.esa.beam.framework.datamodel.Product;

import java.util.List;


public interface ReferenceDatabase {

    /**
     * Retrieves all {link ReferenceMeasurement}s that belong to the given site and
     * are within the bounds of the given product. Each measurement must be within {@code deltaTime}
     * to the time of the reference measurement.
     *
     * @param site      The in-situ site.
     * @param product   The product.
     * @param deltaTime The maximum time difference.
     * @return All reference measurements matching the given conditions.
     */
    List<ReferenceMeasurement> findReferenceMeasurement(String site, Product product, double deltaTime);
}
