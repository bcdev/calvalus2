/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ra;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.calvalus.processing.ra.stat.Extractor;
import com.bc.calvalus.processing.ra.stat.RADateRanges;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * The mapper for the region analysis workflow
 *
 * @author MarcoZ
 */
public class RAMapper extends Mapper<NullWritable, NullWritable, RAKey, RAValue> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        context.progress();

        final Configuration jobConfig = context.getConfiguration();
        final RAConfig raConfig = RAConfig.get(jobConfig);
        if (raConfig == null || raConfig.getInternalRegionNames() == null) {
            throw new IOException("inconsistent region definition, cannot access region names");
        }

        ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        int numRegions = raConfig.getInternalRegionNames().length;
        pm.beginTask("Region Analysis", numRegions * 2);
        try {
            ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, numRegions));
            if (product != null) {
                if (product.getSceneTimeCoding() == null && product.getStartTime() == null && product.getEndTime() == null) {
                    String dateRangesString = jobConfig.get(JobConfigNames.CALVALUS_INPUT_DATE_RANGES);
                    try {
                        RADateRanges dateRanges = RADateRanges.create(dateRangesString);
                        product.setStartTime(ProductData.UTC.parse(dateRanges.formatStart(0), "yyyy-MM-dd HH:mm:ss"));
                        product.setEndTime(ProductData.UTC.parse(dateRanges.formatStart(0), "yyyy-MM-dd HH:mm:ss"));
                        LOG.warning("Product has no time information, assuming " + dateRanges.formatStart(0));
                    } catch (ParseException e) {
                        throw new IOException(e);
                    }
                }
                final AtomicBoolean foundPixel = new AtomicBoolean(false);
                final AtomicLong numObsTotal = new AtomicLong(0);
                final AtomicLong numSamplesTotal = new AtomicLong(0);
                final Set<Integer> regionIdSet = new HashSet<>();
                final String productName = product.getName();
                RARegions.RegionIterator regionIterator = raConfig.createNamedRegionIterator(context.getConfiguration());
                Extractor extractor = new Extractor(product, raConfig.getGoodPixelExpression(), raConfig.getBandNames(), regionIterator) {
                    @Override
                    public void extractedData(int regionIndex, String regionName, long time, int numObs, float[][] samples) throws IOException, InterruptedException {
                        RAKey key = new RAKey(regionIndex, regionName, time);
                        RAValue value = new RAValue(numObs, samples, time, productName);
                        context.write(key, value);

                        int numSamples = samples[0].length;
                        context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Observations").increment(numObs);
                        context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Valid Samples").increment(numSamples);
                        
                        foundPixel.set(true);
                        numObsTotal.addAndGet(numObs);
                        numSamplesTotal.addAndGet(numSamples);
                        regionIdSet.add(regionIndex);
                    }
                };
                extractor.extract(pm);
                if (foundPixel.get()) {
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product with pixel").increment(1);
                    LOG.info("");
                    LOG.info(String.format("total numObs     %12d", numObsTotal.get()));
                    LOG.info(String.format("total numSamples %12d", numSamplesTotal.get()));
                    LOG.info(String.format("total regions    %12d", regionIdSet.size()));
                    LOG.info("");
                }
            } else {
                LOG.warning("product does not cover region, skipping processing.");
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product is empty").increment(1);
            }
        } finally {
            pm.done();
        }
    }

}
