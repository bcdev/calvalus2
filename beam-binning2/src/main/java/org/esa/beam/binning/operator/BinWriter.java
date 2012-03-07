package org.esa.beam.binning.operator;

import com.vividsolutions.jts.geom.Geometry;
import org.esa.beam.binning.Aggregator;
import org.esa.beam.binning.BinningContext;
import org.esa.beam.binning.TemporalBin;
import org.esa.beam.framework.datamodel.ProductData;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFileWriteable;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author Norman Fomferra
 */
public class BinWriter {

    final Logger logger;

    public BinWriter(Logger logger) {
        this.logger = logger;
    }

    public void write(File filePath,
                      List<TemporalBin> temporalBins,
                      BinningContext binningContext,
                      Geometry region,
                      ProductData.UTC startTime,
                      ProductData.UTC stopTime) throws IOException, InvalidRangeException {

        final NetcdfFileWriteable netcdfFile = NetcdfFileWriteable.createNew(filePath.getPath());

        netcdfFile.addGlobalAttribute("global_grid_num_rows", binningContext.getBinningGrid().getNumRows());
        netcdfFile.addGlobalAttribute("super_sampling", binningContext.getSuperSampling());
        if (region != null) {
            netcdfFile.addGlobalAttribute("region", region.toText());
        }
        netcdfFile.addGlobalAttribute("start_time", ProductData.UTC.createDateFormat("yyyy-MM-dd").format(startTime.getAsDate()));
        netcdfFile.addGlobalAttribute("stop_time", ProductData.UTC.createDateFormat("yyyy-MM-dd").format(stopTime.getAsDate()));

        final Dimension binDim = netcdfFile.addDimension("bin", temporalBins.size());
        final int aggregatorCount = binningContext.getBinManager().getAggregatorCount();

        final String varName = "idx";
        final Variable idxVar = netcdfFile.addVariable(varName, DataType.INT, new Dimension[]{binDim});
        final Variable numObsVar = netcdfFile.addVariable("num_obs", DataType.INT, new Dimension[]{binDim});
        final Variable numPassesVar = netcdfFile.addVariable("num_passes", DataType.INT, new Dimension[]{binDim});

        final ArrayList<Variable> featureVars = new ArrayList<Variable>();
        for (int i = 0; i < aggregatorCount; i++) {
            final Aggregator aggregator = binningContext.getBinManager().getAggregator(i);
            final String[] featureNames = aggregator.getTemporalFeatureNames();
            for (String featureName : featureNames) {
                final Variable featureVar = netcdfFile.addVariable(featureName, DataType.FLOAT, new Dimension[]{binDim});
                featureVar.addAttribute(new Attribute("_FillValue", aggregator.getOutputFillValue()));
                featureVars.add(featureVar);
            }
        }

        netcdfFile.create();

        writeVariable(netcdfFile, idxVar, temporalBins, new BinAccessor() {
            @Override
            public void setBuffer(Array buffer, int index, TemporalBin temporalBin) {
                buffer.setInt(index, (int) temporalBin.getIndex());
            }
        });
        writeVariable(netcdfFile, numObsVar, temporalBins, new BinAccessor() {
            @Override
            public void setBuffer(Array buffer, int index, TemporalBin temporalBin) {
                buffer.setInt(index, temporalBin.getNumObs());
            }
        });
        writeVariable(netcdfFile, numPassesVar, temporalBins, new BinAccessor() {
            @Override
            public void setBuffer(Array buffer, int index, TemporalBin temporalBin) {
                buffer.setInt(index, temporalBin.getNumPasses());
            }
        });

        for (int featureIndex = 0; featureIndex < featureVars.size(); featureIndex++) {
            final int k = featureIndex;
            writeVariable(netcdfFile, featureVars.get(k), temporalBins, new BinAccessor() {
                @Override
                public void setBuffer(Array buffer, int index, TemporalBin temporalBin) {
                    buffer.setFloat(index, temporalBin.getFeatureValues()[k]);
                }
            });
        }

        netcdfFile.close();

    }


    private void writeVariable(NetcdfFileWriteable netcdfFile, Variable variable, List<TemporalBin> temporalBins, BinAccessor binAccessor) throws IOException, InvalidRangeException {
        logger.info("Writing variable " + variable.getName() + "...");

        final int BUFFER_SIZE = 4096;
        int[] origin = new int[1];
        Array buffer = Array.factory(variable.getDataType(), new int[]{BUFFER_SIZE});
        int bufferIndex = 0;
        String varName = variable.getName();
        for (TemporalBin temporalBin : temporalBins) {
            if (bufferIndex == BUFFER_SIZE) {
                netcdfFile.write(varName, origin, buffer);
                bufferIndex = 0;
                origin[0] += BUFFER_SIZE;
            }
            binAccessor.setBuffer(buffer, bufferIndex, temporalBin);
            bufferIndex++;
        }
        if (bufferIndex > 0) {
            netcdfFile.write(varName, origin, buffer.section(new int[]{0}, new int[]{bufferIndex}));
        }

        logger.info("Variable " + variable.getName() + " written.");
    }

    private interface BinAccessor {
        void setBuffer(Array buffer, int index, TemporalBin temporalBin);
    }

}
