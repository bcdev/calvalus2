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

package com.bc.calvalus.processing.mosaic;

import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Created by IntelliJ IDEA.
 * User: marcoz
 * Date: 21.10.11
 * Time: 16:59
 * To change this template use File | Settings | File Templates.
 */
public class MosaicFormatterMain {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://cvmaster00:9000");

        configuration.set("calvalus.l3.parameters", "<parameters>\n" +
                "                    <numRows>66792</numRows>\n" +
                "                     <maskExpr>status == 1 or status == 3 or status == 4 or status == 5</maskExpr>\n" +
                "                    <variables>\n" +
                "\t\t              <variable><name>status</name></variable>\n" +
                "            \t\t\t<variable><name>sdr_1</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_2</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_3</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_4</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_5</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_6</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_7</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_8</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_9</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_10</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_11</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_12</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_13</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_14</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_15</name></variable>\n" +
                "\t\t\t            <variable><name>ndvi</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_1</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_2</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_3</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_4</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_5</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_6</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_7</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_8</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_9</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_10</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_11</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_12</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_13</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_14</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_15</name></variable>\n" +
                "                    </variables>" +
                "<aggregators>\n" +
                "                        <aggregator>\n" +
                "                            <type>com.bc.calvalus.processing.mosaic.LCMosaicAlgorithm</type>\n" +
                "                        </aggregator>\n" +
                "                    </aggregators>                    \n" +
                "\t\t  </parameters>");

//configuration.set("calvalus.l3.parameters", "<parameters>\n" +
//                "                    <numRows>66792</numRows>\n" +
//                "                     <maskExpr>status == 1</maskExpr>\n" +
//                "                    <variables>\n" +
//                "\t\t              <variable><name>status</name></variable>\n" +
//                "\t\t\t            <variable><name>sdr_8</name></variable>\n" +
//                "                    </variables>" +
//                "<aggregators>\n" +
//                "                        <aggregator>\n" +
//                "                            <type>com.bc.calvalus.processing.mosaic.LcSDR8MosaicAlgorithm</type>\n" +
//                "                        </aggregator>\n" +
//                "                    </aggregators>                    \n" +
//                "\t\t  </parameters>");

//        configuration.set("calvalus.regionGeometry", "polygon((-2 46, -2 43, 1 43, 1 46, -2 46))");
//        configuration.set("calvalus.regionGeometry", "POLYGON ((-7 54, -7 39, 6 39, 6 54, -7 54))");
        configuration.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF");
        configuration.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "gz");
        configuration.set(JobConfigNames.CALVALUS_OUTPUT_PREFIX, "mosaic");

        MosaicFormatter mosaicFormatter = new MosaicFormatter();
        mosaicFormatter.jobConfig = configuration;
//        mosaicFormatter.setConf(configuration);
        Path part = new Path("hdfs://cvmaster00:9000/calvalus/outputs/lc-production/2005-07-01-10d-lc-sr/part-r-00002");
        mosaicFormatter.process5by5degreeProducts(null, part);
    }
}
