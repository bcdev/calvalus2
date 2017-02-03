package com.bc.calvalus.production;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import org.junit.Ignore;

import java.io.IOException;

/**
 * Test implementation of InventoryService.
 *
 * @author Norman
 */
@Ignore
public class TestInventoryService implements InventoryService {

    @Override
    public ProductSet[] getProductSets(String username, String filter) throws IOException {
        return new ProductSet[]{
                new ProductSet("pt0", "pn0", "pp0"),
                new ProductSet("pt1", "pn1", "pp1"),
        };
    }


}
