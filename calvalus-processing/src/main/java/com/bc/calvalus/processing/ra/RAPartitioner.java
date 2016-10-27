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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * A {@link org.apache.hadoop.mapreduce.Partitioner} for the region analysis extract key.
 *
 * @author MarcoZ
 */
public class RAPartitioner extends Partitioner<RAKey, RAValue> implements Configurable {

    private Configuration conf;
    private int numRegions;

    @Override
    public int getPartition(RAKey key, RAValue value, int numPartitions) {
        return key.getRegionId() * numPartitions / numRegions;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        RAConfig raConfig = RAConfig.get(conf);
        numRegions = raConfig.getRegions().length;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
