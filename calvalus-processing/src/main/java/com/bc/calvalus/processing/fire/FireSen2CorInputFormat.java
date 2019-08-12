package com.bc.calvalus.processing.fire;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.calvalus.processing.productinventory.ProductInventory;
import com.bc.calvalus.processing.productinventory.ProductInventoryEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author thomas
 */
public class FireSen2CorInputFormat extends InputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();

        String inputPathPatterns = conf.get(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS);

        JobClientsMap jobClientsMap = new JobClientsMap(new JobConf(conf));
        HdfsFileSystemService fileSystemService = new HdfsFileSystemService(jobClientsMap);

        List<InputSplit> splits = new ArrayList<>(1000);
        FileStatus[] fileStatuses = getFilteredInputFileStatuses(fileSystemService, inputPathPatterns, conf);

        createSplits(fileStatuses, splits, conf);
        CalvalusLogger.getLogger().info(String.format("Created %d split(s).", splits.size()));
        return splits;
    }

    private FileStatus[] getFilteredInputFileStatuses(HdfsFileSystemService fileSystemService,
                                                      String inputPathPattern, Configuration conf) throws IOException {
        FileStatus[] inputFileStatuses = getFileStatuses(fileSystemService, inputPathPattern, conf);
        List<FileStatus> filteredInputFileStatuses = new ArrayList<>();
        for (FileStatus inputFileStatus : inputFileStatuses) {
            if (!sen2CorExists(inputFileStatus.getPath(), fileSystemService)) {
                filteredInputFileStatuses.add(inputFileStatus);
            } else {
                Logger.getLogger("com.bc.calvalus").info("already exists, skipping");
            }
        }

        return filteredInputFileStatuses.toArray(new FileStatus[0]);
    }

    private boolean sen2CorExists(Path path, FileSystemService fileSystemService) throws IOException {
        // ".../S2A_OPER_PRD_MSIL1C_PDMC_20161201T105416_R106_V20161201T073232_20161201T073232_T37KEA.zip"
        // -->  S2A_OPER_PRD_MSIL2A_PDMC_20161201T105416_R106_V20161201T073232_20161201T073232_T37KEA.tif
        // or
        // ".../S2A_MSIL1C_20161230T110442_N0204_R094_T29QPG_20161230T111111.zip
        // -->  S2A_MSIL2A_20161230T110442_N0204_R094_T29QPG_20161230T111111.tif
        Logger.getLogger("com.bc.calvalus").info("input path: " + path.toString());
        String productName = path.toString().substring(path.toString().lastIndexOf("/") + 1);
            String month;
            String day;
        if (productName.startsWith("S2A_OPER_PRD_MSIL1C")) {
            month = productName.split("_")[5].substring(4, 6);
            day = productName.split("_")[5].substring(6, 8);
        } else if (productName.startsWith("S2A_MSIL1C")) {
            month = productName.split("_")[2].substring(4, 6);
            day = productName.split("_")[2].substring(6, 8);
        } else {
            throw new IllegalStateException("Invalid input path: " + path);
        }
        String sen2CorProductName = productName
                .replace("MSIL1C", "MSIL2A")
                .replace("zip", "tif");

        String sen2CorPath = "hdfs://calvalus/calvalus/projects/fire/s2-pre/2016/" +
                month +
                "/" +
                day +
                "/" +
                sen2CorProductName;

        Logger.getLogger("com.bc.calvalus").info("sen2cor path: " + sen2CorPath);


        return fileSystemService.pathExists(sen2CorPath, "cvop");
    }

    protected void createSplits(FileStatus[] fileStatuses,
                                List<InputSplit> splits,
                                Configuration conf) throws IOException {
        ProductInventory productInventory = ProductInventory.createInventory(conf);
        for (FileStatus file : fileStatuses) {
            long fileLength = file.getLen();
            FileSystem fs = file.getPath().getFileSystem(conf);
            BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, fileLength);
            if (blocks != null && blocks.length > 0) {
                BlockLocation block = blocks[0];
                // create a split for the input
                if (productInventory == null) {
                    // no inventory, process whole product
                    splits.add(new ProductSplit(file.getPath(), fileLength, block.getHosts()));
                } else {
                    ProductInventoryEntry entry = productInventory.getEntry(file.getPath().getName());
                    if (entry != null && entry.getProcessLength() > 0) {
                        // when listed process the given subset
                        int start = entry.getProcessStartLine();
                        int length = entry.getProcessLength();
                        splits.add(new ProductSplit(file.getPath(), fileLength, block.getHosts(), start, length));
                    } else if (entry == null) {
                        // when not listed process whole product
                        splits.add(new ProductSplit(file.getPath(), fileLength, block.getHosts()));
                    }
                }
            } else {
                String msgFormat = "Failed to retrieve block location for file '%s'. Ignoring it.";
                throw new IOException(String.format(msgFormat, file.getPath()));
            }
        }
    }

    private FileStatus[] getFileStatuses(HdfsFileSystemService fileSystemService,
                                         String inputPathPatterns, Configuration conf) throws IOException {

        InputPathResolver inputPathResolver = new InputPathResolver();
        List<String> inputPatterns = inputPathResolver.resolve(inputPathPatterns);
        return fileSystemService.globFileStatuses(inputPatterns, conf);
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }

}
