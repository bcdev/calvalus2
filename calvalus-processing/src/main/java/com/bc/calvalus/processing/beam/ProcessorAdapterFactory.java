/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.framework.datamodel.Product;

import java.awt.Rectangle;

/**
 * Creates a {@code ProcessorAdapter} for the given processor.
 */
public class ProcessorAdapterFactory {

    public static ProcessorAdapter create(Mapper.Context context) {
        return create(context.getInputSplit(), context.getConfiguration());
    }

    public static ProcessorAdapter create(InputSplit inputSplit, Configuration conf) {
        // TODO
        boolean isBeamProcessor = true;
        boolean isShellProcessor = false;
        if (isBeamProcessor) {
            return new BeamProcessorAdapter(inputSplit, conf);
        } else if (isShellProcessor) {
            //TODO
            return null;
        }
        //TODO
        return null;
    }

    /**
     * This only for playing with the API, for understanding it.
     *
     */
    public static void apiUsageExample(Mapper.Context context) throws Exception {
        ProcessorAdapter processorAdapter = ProcessorAdapterFactory.create(context);
        try {
            Geometry roi = null; // from conf

            // MA only: use points from reference data set to restrict roi even further
            Product inputProduct = processorAdapter.getInputProduct();
            Geometry referenceDataRoi = null;
            roi = roi.intersection(referenceDataRoi);


            Rectangle srcProductRect = processorAdapter.computeIntersection(roi);
            if (!srcProductRect.isEmpty()) {
                // all
                processorAdapter.processSourceProduct(srcProductRect);

                // l2 only
                MapContext mapcontext = null;
                String outputFilename = null;
                processorAdapter.saveProcessedProduct(mapcontext, outputFilename);

                // l3 and ma
                Product processedProduct = processorAdapter.openProcessedProduct();
                // do something with the processed product
            }
        } finally {
            processorAdapter.dispose();
        }

    }

}
