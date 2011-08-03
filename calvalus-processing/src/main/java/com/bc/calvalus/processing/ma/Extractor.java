package com.bc.calvalus.processing.ma;

import com.bc.ceres.core.Assert;
import org.esa.beam.framework.datamodel.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Extracts an output record.
 *
 * @author MarcoZ
 * @author Norman
 */
public class Extractor {
    private final Product product;
    private Header header;

    public Extractor(Product product) {
        Assert.notNull(product, "product");
        this.product = product;
        this.header = createHeader();
    }

    private Header createHeader() {
        final List<String> attributeNames = new ArrayList<String>();

        // 0. derived information
        attributeNames.add("product_name");
        attributeNames.add("pixel_x");
        attributeNames.add("pixel_y");
        attributeNames.add("pixel_time");

        // 1. bands
        Band[] productBands = product.getBands();
        for (Band band : productBands) {
            if (!band.isFlagBand()) {
                attributeNames.add(band.getName());
            }
        }

        // 2. flags (virtual bands)
        for (Band band : productBands) {
            if (band.isFlagBand()) {
                FlagCoding flagCoding = band.getFlagCoding();
                String[] flagNames = flagCoding.getFlagNames();
                for (String flagName : flagNames) {
                    attributeNames.add(band.getName() + "." + flagName);
                    product.addBand("flag_" + band.getName() + "_" + flagName, band.getName() + "." + flagName, ProductData.TYPE_INT8);
                }
            }
        }

        // 3. tie-points
        attributeNames.addAll(Arrays.asList(product.getTiePointGridNames()));

        return new Header() {
            @Override
            public String[] getAttributeNames() {
                return attributeNames.toArray(new String[attributeNames.size()]);
            }
        };
    }

    public Record extract(Record input) throws IOException {
        Assert.notNull(input, "input");
        PixelPos pixelPos = product.getGeoCoding().getPixelPos(input.getCoordinate(), null);

        if (pixelPos.isValid() && product.containsPixel(pixelPos)) {
            float[] floatSample = new float[1];
            int[] intSample = new int[1];
            Object[] values = new Object[header.getAttributeNames().length];
            int index = 0;
            values[index++] = product.getName();
            values[index++] = pixelPos.x;
            values[index++] = pixelPos.y;
            values[index++] = "time"; // TODO
            Band[] productBands = product.getBands();
            for (Band band : productBands) {
                if (!band.isFlagBand()) {
                    if (band.isFloatingPointType()) {
                        band.readPixels((int) pixelPos.x, (int) pixelPos.y, 1, 1, floatSample);
                        values[index++] = floatSample[0];
                    } else {
                        band.readPixels((int) pixelPos.x, (int) pixelPos.y, 1, 1, intSample);
                        values[index++] = intSample[0];
                    }
                }
            }
            for (TiePointGrid tiePointGrid : product.getTiePointGrids()) {
                tiePointGrid.readPixels((int) pixelPos.x, (int) pixelPos.y, 1, 1, floatSample);
                values[index++] = floatSample[0];
            }
            return new DefaultRecord(header, input.getCoordinate(), values);
        }
        return null;
    }

    public Header getHeader() {
        return header;
    }

}
