package com.bc.calvalus.inventory.hadoop;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.io.CsvReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HadoopInventoryService implements InventoryService {

    public static final String CALVALUS_INPUT_PATH = "/calvalus/eodata";
    public static final String CALVALUS_OUTPUTS_PATH = "/calvalus/outputs";

    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final DateFormat DATE_FORMAT = ProductData.UTC.createDateFormat(DATE_PATTERN);

    private final FileSystem fileSystem;

    public HadoopInventoryService(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public ProductSet[] getProductSets(String filter) throws Exception {
        Path databasePath = new Path(CALVALUS_INPUT_PATH, "product-sets.csv");
        try {
            ArrayList<ProductSet> productSets = new ArrayList<ProductSet>();
            Path path1 = fileSystem.makeQualified(databasePath);
            InputStream is = fileSystem.open(path1);
            InputStreamReader reader = new InputStreamReader(is);
            CsvReader csvReader = new CsvReader(reader, new char[]{','});
            List<String[]> stringRecords = csvReader.readStringRecords();
            for (String[] record : stringRecords) {
                if (record.length == 3) {
                    String path = record[0];
                    Date date1 = DATE_FORMAT.parse(record[1]);
                    Date date2 = DATE_FORMAT.parse(record[2]);
                    productSets.add(new ProductSet(path, date1, date2));
                }
            }
            return productSets.toArray(new ProductSet[productSets.size()]);
        } catch (Exception e) {
            throw new Exception("Failed to load list of product sets from '" + databasePath + "'.", e);
        }
    }

    @Override
    public String[] getDataInputPaths(String inputGlob) throws IOException {

        Path qualifiedPath = makeQualified(CALVALUS_INPUT_PATH, inputGlob);

        FileStatus[] fileStatuses = fileSystem.globStatus(qualifiedPath);
        String[] resolvedPath = new String[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; i++) {
            resolvedPath[i] = fileStatuses[i].getPath().toString();
        }

        return resolvedPath;
    }

    @Override
    public String getDataOutputPath(String outputPath) {
        return makeQualified(CALVALUS_OUTPUTS_PATH, outputPath).toString();
    }

    private Path makeQualified(String parent, String child) {
        Path path = new Path(child);
        if (!path.isAbsolute()) {
            path = new Path(parent, path);
        }
        return fileSystem.makeQualified(path);
    }

    public String[] globFilePaths(String dirPathGlob) throws IOException {
        FileStatus[] fileStatuses = fileSystem.globStatus(new Path(dirPathGlob));
        String[] paths = new String[fileStatuses.length];
        for (int i = 0; i < fileStatuses.length; i++) {
            paths[i] = fileStatuses[i].getPath().toString();
        }
        return paths;
    }
}
