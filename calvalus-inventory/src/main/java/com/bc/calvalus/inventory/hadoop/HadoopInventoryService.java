package com.bc.calvalus.inventory.hadoop;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.io.CsvReader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HadoopInventoryService implements InventoryService {
    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final DateFormat DATE_FORMAT = ProductData.UTC.createDateFormat(DATE_PATTERN);
    public static final String DATABASE = "/calvalus/eodata/product-sets.csv";

    private final FileSystem fileSystem;

    public HadoopInventoryService(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public ProductSet[] getProductSets(String filter) throws Exception {
        try {
            ArrayList<ProductSet> productSets = new ArrayList<ProductSet>();
            Path path1 = fileSystem.makeQualified(new Path(DATABASE));
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
            throw new Exception("Failed to load list of product sets from '" + DATABASE + "'.", e);
        }
    }

}
