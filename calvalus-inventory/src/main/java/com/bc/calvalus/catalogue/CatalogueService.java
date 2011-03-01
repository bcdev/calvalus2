package com.bc.calvalus.catalogue;

/**
 * The interface to the Calvalus catalogue service.
 * <p/>
 * TODO - make use of this interface
 *
 * @author Norman
 */
public interface CatalogueService {
    /**
     * Gets the product set for the given ID.
     *
     * @param id The product set ID.
     * @return The product set, or {@code null} if a product set with the given ID does not exist..
     */
    ProductSet getProductSet(String id) throws Exception;

    /**
     * Gets all product sets.
     *
     * @return The array product sets, which may be empty.
     */
    ProductSet[] getProductSets() throws Exception;
}
