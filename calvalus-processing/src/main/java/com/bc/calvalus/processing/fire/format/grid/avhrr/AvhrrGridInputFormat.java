package com.bc.calvalus.processing.fire.format.grid.avhrr;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.collocation.CollocateOp;
import org.esa.snap.collocation.ResamplingType;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class AvhrrGridInputFormat extends InputFormat {

    public static void main(String[] args) throws IOException {
        ReprojectionOp reprojectionOp = new ReprojectionOp();
        reprojectionOp.setSourceProduct(ProductIO.readProduct("C:\\ssd\\ltdr\\BA_1993_1_Dates.tif"));
        reprojectionOp.setParameterDefaultValues();
        reprojectionOp.setParameter("crs", "EPSG:4326");
        reprojectionOp.setParameter("width", "7200");
        reprojectionOp.setParameter("height", "3600");
//        ProductIO.writeProduct(reprojectionOp.getTargetProduct(), "C:\\ssd\\ltdr\\reproj.nc", "NetCDF4-CF");

        Product lcProduct = ProductIO.readProduct("c:\\ssd\\ltdr\\ESACCI-LC-L4-LCCS-Map-300m-P5Y-2000-v1.6.1.nc");

        SubsetOp subsetOp = new SubsetOp();
        subsetOp.setParameterDefaultValues();
        subsetOp.setSourceProduct(lcProduct);
        subsetOp.setBandNames(new String[]{"lccs_class"});

        CollocateOp collocateOp = new CollocateOp();
        collocateOp.setMasterProduct(reprojectionOp.getTargetProduct());
        collocateOp.setSlaveProduct(subsetOp.getTargetProduct());
        collocateOp.setResamplingType(ResamplingType.NEAREST_NEIGHBOUR);

        ProductIO.writeProduct(collocateOp.getTargetProduct(), "c:\\ssd\\ltdr\\lc.nc", "NetCDF4-CF");
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String year = conf.get("calvalus.year");
        String month = conf.get("calvalus.month");
        if (Integer.parseInt(month) < 10) {
            month = "" + Integer.parseInt(month);
        }
        List<InputSplit> splits = new ArrayList<>(1);
        String inputPathPattern = "hdfs://calvalus/calvalus/projects/fire/avhrr-ba/BA_" + year + "_" + month + "_.*_WGS.tif";
        FileStatus[] fileStatuses = getFileStatuses(inputPathPattern, conf);
        List<FileStatus> fileStatusList = Arrays.asList(fileStatuses);
        fileStatusList.sort(Comparator.comparing(o -> o.getPath().getName()));
        for (int i = 0; i < 162; i++) {
            addSplit(fileStatusList, splits, i);
        }
        return splits;
    }

    private void addSplit(List<FileStatus> fileStatuses, List<InputSplit> splits, int index) {
        List<Path> filePaths = new ArrayList<>();
        List<Long> fileLengths = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
            Path path = fileStatus.getPath();
            filePaths.add(path);
            fileLengths.add(fileStatus.getLen());
        }
        filePaths.add(new Path("" + index));
        fileLengths.add(0L);

        CombineFileSplit combineFileSplit = new CombineFileSplit(filePaths.toArray(new Path[filePaths.size()]),
                fileLengths.stream().mapToLong(Long::longValue).toArray());
        splits.add(combineFileSplit);
    }

    private FileStatus[] getFileStatuses(String inputPathPatterns,
                                         Configuration conf) throws IOException {

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsFileSystemService hdfsInventoryService = new HdfsFileSystemService(jobClientsMap);
        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return hdfsInventoryService.globFileStatuses(inputPatterns, conf);
    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }

}
