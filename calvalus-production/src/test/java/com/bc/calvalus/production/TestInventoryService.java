package com.bc.calvalus.production;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import org.junit.Ignore;

/**
 * Test implementation of InventoryService.
 *
 * @author Norman
 */
@Ignore
public class TestInventoryService implements InventoryService {
    @Override
    public ProductSet[] getProductSets(String filter) throws Exception {
        return new ProductSet[]{
                new ProductSet("ps0", null, null),
                new ProductSet("ps1", null, null),
        };
    }

    @Override
    public String[] getDataInputPaths(String inputGlob) {
        if (inputGlob.contains("*")) {
            throw new IllegalArgumentException("Hey, wildcards are not supported! This is a test class!");
        }
        if (!inputGlob.startsWith("/")) {
            inputGlob = "/calvalus/eodata/" + inputGlob;
        }
        return new String[] {"hdfs://cvmaster00:9000" + inputGlob};
    }

    @Override
    public String getDataOutputPath(String outputPath) {
        if (!outputPath.startsWith("/")) {
            outputPath = "/calvalus/outputs/" + outputPath;
        }
        return "hdfs://cvmaster00:9000" + outputPath;
    }

}
