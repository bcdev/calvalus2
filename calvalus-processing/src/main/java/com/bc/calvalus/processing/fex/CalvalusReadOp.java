/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.fex;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorException;
import org.esa.beam.framework.gpf.OperatorSpi;
import org.esa.beam.framework.gpf.Tile;
import org.esa.beam.framework.gpf.annotations.OperatorMetadata;
import org.esa.beam.framework.gpf.annotations.Parameter;
import org.esa.beam.framework.gpf.annotations.TargetProduct;

import java.awt.Rectangle;
import java.io.IOException;

/**
 * A GPF read OP, made for Calvalus.
 */
@OperatorMetadata(alias = "Read",
                  version = "1.1",
                  authors = "Marco Zuehlke, Norman Fomferra",
                  copyright = "(c) 2014 by Brockmann Consult",
                  description = "Reads a product from HDFS or from disk.")
public class CalvalusReadOp extends Operator {

    @Parameter(description = "The file from which the data product is read.", notNull = true, notEmpty = true)
    private String file;
    @TargetProduct
    private Product targetProduct;

    private transient Configuration configuration;
    private transient ProductReader productReader;

    private void setHadoopConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }


    @Override
    public void initialize() throws OperatorException {
        try {
            String inputFormat = configuration.get(JobConfigNames.CALVALUS_INPUT_FORMAT, null);
            targetProduct = CalvalusProductIO.readProduct(new Path(file), configuration, inputFormat);
            productReader = targetProduct.getProductReader();
        } catch (IOException e) {
            throw new OperatorException(e);
        }
    }

    @Override
    public void computeTile(Band band, Tile targetTile, ProgressMonitor pm) throws OperatorException {

        ProductData dataBuffer = targetTile.getRawSamples();
        Rectangle rectangle = targetTile.getRectangle();
        try {
            productReader.readBandRasterData(band, rectangle.x, rectangle.y, rectangle.width,
                                             rectangle.height, dataBuffer, pm);
            targetTile.setRawSamples(dataBuffer);
        } catch (IOException e) {
            throw new OperatorException(e);
        }
    }

    public static class Spi extends OperatorSpi {
        private final Configuration configuration;

        public Spi(Configuration configuration) {
            super(CalvalusReadOp.class);
            this.configuration = configuration;
        }

        public Operator createOperator() throws OperatorException {
            try {
                final CalvalusReadOp operator = (CalvalusReadOp) getOperatorClass().newInstance();
                operator.setSpi(this);
                operator.setParameterDefaultValues();
                operator.setHadoopConfiguration(configuration);
                return operator;
            } catch (InstantiationException | IllegalAccessException e) {
                throw new OperatorException(e);
            }
        }
    }
}
