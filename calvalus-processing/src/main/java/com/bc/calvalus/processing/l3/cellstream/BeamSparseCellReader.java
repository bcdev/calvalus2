package com.bc.calvalus.processing.l3.cellstream;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.io.LongWritable;
import org.esa.snap.framework.datamodel.ProductData;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Section;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

/**
 * Reads binned product written by the BEAM binner2 in sparse format.
 */
public class BeamSparseCellReader extends AbstractNetcdfCellReader {

    private static final int DEFAULT_READAHEAD = 1000;
    private static final DateFormat DATE_FORMAT = ProductData.UTC.createDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private final String[] featureNames;
    private final int numBins;
    private final Date startDate;
    private final Date endDate;
    private int numRows;

    private Variable binNumVar;
    private Array binNumArray;
    private Variable numObsVar;
    private Array numObsArray;
    private Variable numSceneVar;
    private Array numSceneArray;
    private final Variable[] featureVars;
    private final Array[] featureArrays;

    private int currentBinIndex = 0;
    private int arrayPointer = Integer.MAX_VALUE;
    private int readAhead;

    public BeamSparseCellReader(NetcdfFile netcdfFile) {
        super(netcdfFile);
        numBins = readLargestDimensionSize(netcdfFile);
        List<String> featureNameList = new ArrayList<String>();
        List<Variable> featureVarsList = new ArrayList<Variable>();

        for (Variable variable : netcdfFile.getVariables()) {
            final String variableName = variable.getFullName();
            if (variable.getDimensions().get(0).getLength() == numBins) {
                if (variableName.equals("bl_bin_num")) {
                    binNumVar = variable;
                } else if (variableName.equals("bl_nscenes")) {
                    numSceneVar = variable;
                } else if (variableName.equals("bl_nobs")) {
                    numObsVar = variable;
                } else {
                    featureNameList.add(variableName);
                    featureVarsList.add(variable);
                }

                Attribute chunkSize = variable.findAttribute("_ChunkSize");
                if (chunkSize != null && chunkSize.getNumericValue() != null) {
                    readAhead = chunkSize.getNumericValue().intValue();
                }
            }
        }
        if (readAhead == 0) {
            readAhead = DEFAULT_READAHEAD;
        }
        featureVars = featureVarsList.toArray(new Variable[featureVarsList.size()]);
        featureArrays = new Array[featureVarsList.size()];
        featureNames = featureNameList.toArray(new String[featureNameList.size()]);

        startDate = extractDate(netcdfFile, "start_time", "time_coverage_start");
        endDate = extractDate(netcdfFile, "stop_time", "time_coverage_end");

        final Dimension bin_index = netcdfFile.findDimension("bin_index");
        if (bin_index != null) {
            numRows = bin_index.getLength();
        }
    }

    private static Date extractDate(NetcdfFile netcdfFile, String name, String alternativeName) {
        Attribute timeAttribute = netcdfFile.findGlobalAttribute(name);
        if (timeAttribute == null) {
            timeAttribute = netcdfFile.findGlobalAttribute(alternativeName);
        }
        if (timeAttribute == null) {
            return null;
        }
        try {
            return DATE_FORMAT.parse(timeAttribute.getStringValue());
        } catch (ParseException e) {
            CalvalusLogger.getLogger().log(Level.WARNING, "failed to parse date. " + e.getMessage(), e);
        }
        return null;
    }

    @Override
    public int getNumRows() {
        return numRows;
    }

    @Override
    public String[] getFeatureNames() {
        return featureNames;
    }

    @Override
    public int getNumReadBins() {
        return currentBinIndex;
    }

    @Override
    public int getNumBins() {
        return numBins;
    }

    @Override
    public Date getStartDate() {
        return startDate;
    }

    @Override
    public Date getEndDate() {
        return endDate;
    }

    @Override
    public boolean readNext(LongWritable key, L3TemporalBin temporalBin) throws Exception {
        if (currentBinIndex >= numBins) {
            return false;
        }
        if (arrayPointer >= readAhead) {
            final int origin = currentBinIndex;
            final int shape = Math.min(numBins - currentBinIndex, readAhead);
            readAhead(new Section(new int[]{origin}, new int[]{shape}));
            arrayPointer = 0;
        }
        long binIndex = binNumArray.getLong(arrayPointer);
        key.set(binIndex);
        temporalBin.setIndex(binIndex);
        temporalBin.setNumObs(numObsArray.getInt(arrayPointer));
        temporalBin.setNumPasses(numSceneArray.getInt(arrayPointer));
        float[] featureValues = temporalBin.getFeatureValues();
        for (int i = 0; i < featureArrays.length; i++) {
            featureValues[i] = featureArrays[i].getFloat(arrayPointer);
        }
        arrayPointer++;
        currentBinIndex++;
        return true;
    }

    private void readAhead(Section section) throws IOException, InvalidRangeException {
        binNumArray = binNumVar.read(section);
        numObsArray = numObsVar.read(section);
        numSceneArray = numSceneVar.read(section);
        for (int i = 0; i < featureVars.length; i++) {
            featureArrays[i] = featureVars[i].read(section);
        }
    }

    private int readLargestDimensionSize(NetcdfFile netcdfFile) {
        int largestDimensionSize = 0;
        for (Dimension dimension : netcdfFile.getDimensions()) {
            if (dimension.getLength() > largestDimensionSize) {
                largestDimensionSize = dimension.getLength();
            }
        }
        return largestDimensionSize;
    }
}
