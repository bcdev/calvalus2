package com.bc.calvalus.production;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import org.junit.Ignore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

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
                new ProductSet(null, "ps0", "ps0", null, null, "", null),
                new ProductSet(null, "ps1", "ps1", null, null, "", null),
        };
    }

    @Override
    public String[] globPaths(List<String> pathPatterns) {
        String[] inputPathes = new String[pathPatterns.size()];
        for (int i = 0; i < pathPatterns.size(); i++) {
            String inputRegex = pathPatterns.get(i);
            if (inputRegex.contains("*")) {
                throw new IllegalArgumentException("Hey, wildcards are not supported! This is a test class!");
            }
            if (!inputRegex.startsWith("/")) {
                inputRegex = "/calvalus/eodata/" + inputRegex;
            }
            inputPathes[i] = "hdfs://master00:9000" + inputRegex;
        }
        return inputPathes;
    }

    @Override
    public String getQualifiedPath(String outputPath) {
        if (!outputPath.startsWith("/")) {
            outputPath = "/calvalus/outputs/" + outputPath;
        }
        return "hdfs://master00:9000" + outputPath;
    }

    @Override
    public OutputStream addFile(String userPath) throws IOException {
        return null;
    }

    @Override
    public boolean removeFile(String userPath) throws IOException {
        return false;
    }

    @Override
    public boolean removeDirectory(String userPath) throws IOException {
        return false;
    }
}
