package com.bc.calvalus.processing.l3.cellstream;

import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.io.LongWritable;
import org.esa.snap.binning.PlanetaryGrid;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.support.SeadasGrid;
import org.esa.snap.core.datamodel.ProductData;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Structure;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
    private int numRows;

    private Variable binNumVar;
    private Array binNumArray;
    private Variable numObsVar;
    private Array numObsArray;
    private Variable numSceneVar;
    private Array numSceneArray;
    private final Variable[] featureVars;
    private final Array[] featureArrays;

    private int currentBinIndex;
    private int currentBlockPointer = Integer.MAX_VALUE;
    private final Date startDate;
    private final Date endDate;
    private final SeadasGrid seadasGrid;
    private int[] rowOffset;
    private int[] rowExtent;
    private int[] blockOffset;
    private int[] blockExtent;

    private int currentRow;
    private int currentRowPointer;
    private int currentRowElems;
    private int currentBlock;
    private int readAheadElems;
    private int numReadBins;


    public SeadasBinnnedCellReader(NetcdfFile netcdfFile) throws IOException {
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

        featureVars = featureVarsList.toArray(new Variable[featureVarsList.size()]);
        featureArrays = new Array[featureVarsList.size()];
        featureNames = featureNameList.toArray(new String[featureNameList.size()]);

        startDate = extractDate(netcdfFile, "Start_Day", "Start_Year");
        endDate = extractDate(netcdfFile, "End_Day", "End_Year");

        Variable binIndexVariable = netcdfFile.findVariable("Level-3_Binned_Data/BinIndex");
        List<Variable> variables = ((Structure) binIndexVariable).getVariables();
        Array extentArray = null;
        for (Variable variable : variables) {
            if (variable.getShortName().equals("extent")) {
                extentArray = variable.read();
                break;
            }
        }
        numRows = binIndexVariable.getShape(0);
        rowOffset = new int[numRows];
        rowExtent = new int[numRows];
        blockOffset = new int[numRows];
        blockExtent = new int[numRows];

        int rowOff = 0;
        int blockOff = 0;
        int blockSize = 0;
        int rowCounter = 0;
        int blockCounter = 0;
        for (int i = 0; i < numRows; i++) {
            int extent = extentArray.getInt(i);
            if (extent != 0) {
                rowOffset[rowCounter] = rowOff;
                rowExtent[rowCounter] = extent;
                rowOff += extent;
                rowCounter++;
                blockSize += extent;
                if (blockSize > DEFAULT_READAHEAD) {
                    blockExtent[blockCounter] = blockSize;
                    blockOffset[blockCounter] = blockOff;
                    blockOff += blockSize;
                    blockSize = 0;
                    blockCounter++;
                }
            }
        }
        if (blockSize > 0) {
            blockExtent[blockCounter] = blockSize;
            blockOffset[blockCounter] = blockOff;
            blockCounter++;
        }

        rowExtent = Arrays.copyOf(rowExtent, rowCounter);
        rowOffset = Arrays.copyOf(rowOffset, rowCounter);
        blockExtent = Arrays.copyOf(blockExtent, blockCounter);
        blockOffset = Arrays.copyOf(blockOffset, blockCounter);

        currentRow = rowExtent.length - 1;
        currentBlock = blockExtent.length - 1;

        currentBinIndex = numBins;
        currentBlockPointer = Integer.MAX_VALUE;
        readAheadElems = 0;

        currentRowPointer = Integer.MAX_VALUE;
        currentRowElems = 0;

        numReadBins = 0;

        BinningConfig binningConfig = new BinningConfig();
        binningConfig.setNumRows(numRows);
        PlanetaryGrid planetaryGrid = binningConfig.createPlanetaryGrid();
        seadasGrid = new SeadasGrid(planetaryGrid);
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
    public int getNumRows() {
        return numRows;
    }

    @Override
    public String[] getFeatureNames() {
        return featureNames;
    }

    @Override
    public int getNumReadBins() {
        return numReadBins;
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
        if (currentBlock == -1 && currentRowPointer >=  currentRowElems && currentRow == -1) {
            return false;
        }

        // read ahead buffer is empty
        if (currentBlockPointer >=  readAheadElems) {
            int toRead = blockExtent[currentBlock];
            int origin = blockOffset[currentBlock];
            currentBlock--;
            readAhead(new int[]{origin}, new int[]{toRead});
            readAheadElems = toRead;
            currentBlockPointer = 0;
        }
        // new row
        if (currentRowPointer >=  currentRowElems) {
            currentRowPointer = 0;
            currentRowElems = rowExtent[currentRow];
            currentBinIndex = rowOffset[currentRow];
            currentRow--;
        }

        long seadasBinIndex = binNumArray.getLong(currentBinIndex);
        long beamBinIndex = seadasGrid.reverseBinIndex(seadasBinIndex);
        key.set(beamBinIndex);

        temporalBin.setIndex(beamBinIndex);
        temporalBin.setNumObs(numObsArray.getInt(currentBinIndex));
        temporalBin.setNumPasses(numSceneArray.getInt(currentBinIndex));
        float[] featureValues = temporalBin.getFeatureValues();
        for (int i = 0; i < featureArrays.length; i++) {
            featureValues[i] = featureArrays[i].getFloat(currentBinIndex);
        }

        currentBlockPointer++;
        currentRowPointer++;
        currentBinIndex++;
        numReadBins++;
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
