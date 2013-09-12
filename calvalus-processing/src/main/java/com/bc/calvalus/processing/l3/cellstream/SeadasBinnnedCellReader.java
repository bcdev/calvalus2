package com.bc.calvalus.processing.l3.cellstream;

import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.io.LongWritable;
import org.esa.beam.framework.datamodel.ProductData;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Structure;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Reads binned product written by the SEADAS l2bin.
 */
public class SeadasBinnnedCellReader extends AbstractNetcdfCellReader {

    private static final int DEFAULT_READAHEAD = 10000;

    private final String[] featureNames;
    private final int numBins;

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
    private final Date startDate;
    private final Date endDate;

    public SeadasBinnnedCellReader(NetcdfFile netcdfFile) {
        super(netcdfFile);
        numBins = readLargestDimensionSize(netcdfFile);
        List<String> featureNameList = new ArrayList<String>();
        List<Variable> featureVarsList = new ArrayList<Variable>();

        binNumVar = netcdfFile.findVariable("Level-3_Binned_Data/BinList.bin_num");
        numSceneVar = netcdfFile.findVariable("Level-3_Binned_Data/BinList.nscenes");
        numObsVar = netcdfFile.findVariable("Level-3_Binned_Data/BinList.nobs");

        for (Variable variable : netcdfFile.getVariables()) {
            if (variable.getDimension(0) != null &&
                    variable.getDimension(0).getLength() == numBins &&
                    !variable.getShortName().equals("BinList") &&
                    variable instanceof Structure) {
                Structure structure = (Structure) variable;
                for (Variable structVariable : structure.getVariables()) {
                    featureNameList.add(structVariable.getShortName());
                    featureVarsList.add(structVariable);
                }
            }
        }
        Variable weights = netcdfFile.findVariable("Level-3_Binned_Data/BinList.weights");
        featureNameList.add(weights.getShortName());
        featureVarsList.add(weights);

        readAhead = DEFAULT_READAHEAD;
        featureVars = featureVarsList.toArray(new Variable[featureVarsList.size()]);
        featureArrays = new Array[featureVarsList.size()];
        featureNames = featureNameList.toArray(new String[featureNameList.size()]);

        startDate = extractDate(netcdfFile, "Start_Day", "Start_Year");
        endDate = extractDate(netcdfFile, "End_Day", "End_Year");
    }

    private static Date extractDate(NetcdfFile netcdfFile, String dayName, String yearName) {
        int day = netcdfFile.findGlobalAttribute(dayName).getNumericValue().intValue();
        int year = netcdfFile.findGlobalAttribute(yearName).getNumericValue().intValue();
        Calendar calendar = ProductData.UTC.createCalendar();
        calendar.setTimeInMillis(0);
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.DAY_OF_YEAR, day);
        return calendar.getTime();
    }

    @Override
    public String[] getFeatureNames() {
        return featureNames;
    }

    @Override
    public int getCurrentIndex() {
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
            readAhead(new int[]{origin}, new int[]{shape});
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

    private void readAhead(int[] origin, int[] shape) throws IOException, InvalidRangeException {
        binNumArray = binNumVar.read().section(origin, shape);
        numObsArray = numObsVar.read().section(origin, shape);
        numSceneArray = numSceneVar.read().section(origin, shape);
        for (int i = 0; i < featureVars.length; i++) {
            featureArrays[i] = featureVars[i].read().section(origin, shape);
        }
    }

    private int readLargestDimensionSize(NetcdfFile netcdfFile) {
        Variable binList = netcdfFile.findVariable("Level-3_Binned_Data/BinList");
        return binList.getDimension(0).getLength();
    }

}
