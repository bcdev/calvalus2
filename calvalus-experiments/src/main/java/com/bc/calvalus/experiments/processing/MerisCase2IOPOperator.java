/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.experiments.processing;

import org.esa.beam.atmosphere.operator.GlintCorrectionOperator;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorException;
import org.esa.beam.framework.gpf.OperatorSpi;
import org.esa.beam.framework.gpf.annotations.OperatorMetadata;
import org.esa.beam.framework.gpf.annotations.SourceProduct;
import org.esa.beam.meris.case2.RegionalWaterOp;
import org.esa.beam.meris.radiometry.MerisRadiometryCorrectionOp;
import org.esa.beam.meris.radiometry.equalization.ReprocessingVersion;

import static org.esa.beam.dataio.envisat.EnvisatConstants.*;


/**
 * An operator that computes case 2 IOPs from MERIS L1b products.
 * This includes radiometric corrections, atmospheric correction and the actual IOP retrieval. <p>
 *
 * Migrated from case2r to calvalus, deleted in case2r
 */
@OperatorMetadata(alias = "Calvalus.Case2IOP",
                  description = "Performs IOP retrieval on L1b MERIS products, including radiometric correction and atmospheric correction.",
                  authors = "Roland Doerffer (GKSS); Marco Peters (Brockmann Consult)",
                  copyright = "(c) 2010 by Brockmann Consult",
                  version = "1.0",
                  internal = true)

public class MerisCase2IOPOperator extends Operator {

    @SourceProduct(alias = "source", label = "Name", description = "The source product.",
                   bands = {
                           MERIS_L1B_FLAGS_DS_NAME, MERIS_DETECTOR_INDEX_DS_NAME,
                           MERIS_L1B_RADIANCE_1_BAND_NAME,
                           MERIS_L1B_RADIANCE_2_BAND_NAME,
                           MERIS_L1B_RADIANCE_3_BAND_NAME,
                           MERIS_L1B_RADIANCE_4_BAND_NAME,
                           MERIS_L1B_RADIANCE_5_BAND_NAME,
                           MERIS_L1B_RADIANCE_6_BAND_NAME,
                           MERIS_L1B_RADIANCE_7_BAND_NAME,
                           MERIS_L1B_RADIANCE_8_BAND_NAME,
                           MERIS_L1B_RADIANCE_9_BAND_NAME,
                           MERIS_L1B_RADIANCE_10_BAND_NAME,
                           MERIS_L1B_RADIANCE_11_BAND_NAME,
                           MERIS_L1B_RADIANCE_12_BAND_NAME,
                           MERIS_L1B_RADIANCE_13_BAND_NAME,
                           MERIS_L1B_RADIANCE_14_BAND_NAME,
                           MERIS_L1B_RADIANCE_15_BAND_NAME
                   })
    private Product sourceProduct;

    @Override
    public void initialize() throws OperatorException {
        Operator radCorOp = new MerisRadiometryCorrectionOp();
        radCorOp.setParameter("doCalibration", false);
        radCorOp.setParameter("doRadToRefl", false);
        radCorOp.setParameter("reproVersion", ReprocessingVersion.REPROCESSING_2);
        radCorOp.setSourceProduct("sourceProduct", sourceProduct);

        Operator atmoCorOp = new GlintCorrectionOperator();
        atmoCorOp.setSourceProduct("merisProduct", radCorOp.getTargetProduct());

        Operator case2Op = new RegionalWaterOp();
        case2Op.setSourceProduct("acProduct", atmoCorOp.getTargetProduct());

        setTargetProduct(case2Op.getTargetProduct());
    }

    public static class Spi extends OperatorSpi {

        public Spi() {
            super(MerisCase2IOPOperator.class);
        }
    }

}
