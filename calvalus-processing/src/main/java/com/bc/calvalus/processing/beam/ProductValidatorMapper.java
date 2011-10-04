/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.beam;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.shellexec.ProcessorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.framework.datamodel.Product;

import java.io.IOException;

/**
 * A mapper that checks products for validity.
 */
public class ProductValidatorMapper extends Mapper<NullWritable, NullWritable, Text /*error message*/, Text /*N1 input path name*/> {

    @Override
    public void run(Context context) throws IOException, InterruptedException, ProcessorException {
        Configuration jobConfig = context.getConfiguration();
        ProductFactory productFactory = new ProductFactory(jobConfig);

        final FileSplit split = (FileSplit) context.getInputSplit();
        // parse request
        Path inputPath = split.getPath();

        long length = split.getLength();
        if (length <= 12029L) {
            context.write(new Text("Input file to small"), new Text(inputPath.toString()));
            return;
        }

        Product sourceProduct;
        try {
            String inputFormat = jobConfig.get(JobConfigNames.CALVALUS_INPUT_FORMAT, null);
            sourceProduct = productFactory.readProduct(inputPath, inputFormat);
        } catch (IOException ioe) {
            context.write(new Text("Failed to read product: " + ioe.getMessage()), new Text(inputPath.toString()));
            return;
        }

        try {
            if (ProductFactory.productHasEmptyTiepoints(sourceProduct)) {
                context.write(new Text("Product has empty tie-points"), new Text(inputPath.toString()));
            } else {
                context.write(new Text("OK"), new Text(inputPath.toString()));
            }
        } finally {
            sourceProduct.dispose();
        }
    }
}
