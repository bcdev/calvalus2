package com.bc.calvalus.production.local;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;

import java.io.File;
import java.util.List;

/**
*
*/
public class LocalInventoryService implements InventoryService {

    public static final File INPUT_DIR = new File(System.getProperty("user.home"), ".calvalus/test-input-data");
    public static final File OUTPUT_FILE = new File(System.getProperty("user.home"), ".calvalus/test-output-data");

    private ProductSet[] productSets;

    LocalInventoryService(ProductSet... productSets) {
        this.productSets = productSets;
        INPUT_DIR.mkdirs();
        OUTPUT_FILE.mkdirs();
    }

    @Override
    public ProductSet[] getProductSets(String filter) throws Exception {
        return productSets;
    }

    @Override
    public String[] getDataInputPaths(List<String> inputRegexs) {
        return new String[] { INPUT_DIR.getPath() };
    }

    @Override
    public String getDataOutputPath(String outputPath) {
        return OUTPUT_FILE.getPath();
    }
}
