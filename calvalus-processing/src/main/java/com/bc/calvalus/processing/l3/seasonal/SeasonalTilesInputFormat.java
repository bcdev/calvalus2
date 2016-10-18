package com.bc.calvalus.processing.l3.seasonal;

import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.hadoop.PatternBasedInputFormat;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.calvalus.processing.productinventory.ProductInventory;
import com.bc.calvalus.processing.productinventory.ProductInventoryEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An input format that maps each stack of weekly tiles to a single (file) split containing only the first tile.
 * <p/>
 * Input files are given by the configuration parameter
 * {@link com.bc.calvalus.processing.JobConfigNames#CALVALUS_INPUT_PATH_PATTERNS CALVALUS_INPUT_PATH_PATTERNS}.
 * Its value is expected to be a comma-separated list of file path patterns (HDFS URLs).
 * These patterns can contain dates and region names.
 *
 * @author Martin
 */
public class SeasonalTilesInputFormat extends PatternBasedInputFormat {

    // ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v08-20090108-v2.0.nc
    static final Pattern SR_FILENAME_PATTERN =
            Pattern.compile("ESACCI-LC-L3-SR-[^-]*-[^-]*-[^-]*-(h[0-9][0-9]v[0-9][0-9])-........-[^-]*.nc");

    @Override
    protected void createSplits(ProductInventory productInventory,
                                FileStatus[] fileStatuses,
                                List<InputSplit> splits,
                                Configuration conf) throws IOException {
        for (FileStatus file : fileStatuses) {
            if (tileNameIn(tileNameOf(file.getPath().getName()), splits)) {
                continue;
            }
            LOG.info("tile " + file.getPath().getName() + " represents " + tileNameOf(file.getPath().getName()));
            long fileLength = file.getLen();
            FileSystem fs = getFileSystem(file, conf);
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

    protected FileSystem getFileSystem(FileStatus file, Configuration conf) throws IOException {
        return file.getPath().getFileSystem(conf);
    }

    private static boolean tileNameIn(String tileName, List<InputSplit> splits) {
        for (InputSplit split : splits) {
            if (tileName.equals(tileNameOf(((FileSplit)split).getPath().getName()))) {
                return true;
            }
        }
        return false;
    }

    private static String tileNameOf(String fileName) {
        Matcher matcher = SR_FILENAME_PATTERN.matcher(fileName);
        if (! matcher.matches()) {
            throw new IllegalArgumentException("file name does not match pattern " + SR_FILENAME_PATTERN.pattern() + ": " + fileName);
        }
        return matcher.group(1);
    }

    // multi-year variant
    protected FileStatus[] getFileStatuses(HdfsFileSystemService hdfsFileSystemService,
                                           String inputPathPatterns,
                                           Date minDate,
                                           Date maxDate,
                                           String regionName,
                                           Configuration conf) throws IOException {
        InputPathResolver inputPathResolver = new InputPathResolver();
        inputPathResolver.setMinDate(minDate);
        inputPathResolver.setMaxDate(maxDate);
        inputPathResolver.setRegionName(regionName);
        List<String> inputPatterns = inputPathResolver.resolveMultiYear(inputPathPatterns);
        return hdfsFileSystemService.globFileStatuses(inputPatterns, conf);
    }
}
