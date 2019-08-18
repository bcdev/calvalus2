package com.bc.calvalus.processing.hadoop;

import com.bc.calvalus.commons.InputPathResolver;
import com.bc.calvalus.processing.productinventory.ProductInventory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
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
public class GranuleTilesInputFormat extends PatternBasedInputFormat {

    // S2A_OPER_PRD_MSIL1C_PDMC_20160601T202439_R069_V20160601T170312_20160601T171125_T14QND.zip
    // S2A_MSIL1C_20170601T174911_N0205_R141_T12RWU_20170601T180009.zip
    static final Pattern FILENAME_PATTERN0 =
            Pattern.compile("S2._(?:OPER_PRD_MSIL1C_PDMC_........T......_R..._V........T......_........T......|" +
                                    "MSIL1C_........T......_N...._R...)" +
                                    "_(T.....)" +
                                    "(?:|_........T......)" +
                                    ".zip");
    static final Pattern FILENAME_PATTERN =
            Pattern.compile("(S2.)" +
                                    "_(?:OPER_PRD_MSIL1C_PDMC_........T......|MSIL1C_........T......_N....)" +
                                    "_(R...)" +
                                    "(?:_V........T......_........T......|)" +
                                    "_(T.....)" +
                                    "(?:|_........T......)" +
                                    ".zip");
    @Override
    protected void createSplits(ProductInventory productInventory,
                                RemoteIterator<LocatedFileStatus> fileStatusIt,
                                List<InputSplit> splits,
                                Configuration conf, int requestSizeLimit, boolean withDirs) throws IOException {
        while (fileStatusIt.hasNext()) {
            LocatedFileStatus locatedFileStatus = fileStatusIt.next();
            String fileName = locatedFileStatus.getPath().getName();
            if (tileNameIn(tileNameOf(fileName), splits)) {
                continue;
            }
            LOG.info("tile " + fileName + " represents " + tileNameOf(fileName));
            InputSplit split = createSplit(productInventory, conf, locatedFileStatus, withDirs);
            if (split != null) {
                splits.add(split);
                if (requestSizeLimit > 0 && splits.size() == requestSizeLimit) {
                    break;
                }
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
        Matcher matcher = FILENAME_PATTERN.matcher(fileName);
        if (! matcher.matches()) {
            throw new IllegalArgumentException("file name does not match pattern " + FILENAME_PATTERN.pattern() + ": " + fileName);
        }
        return matcher.group(3) + '_' + matcher.group(2) + '_' + matcher.group(1);
    }

    // multi-year variant
    protected List<String> getInputPatterns(String inputPathPatterns, Date minDate, Date maxDate, String regionName) {
        InputPathResolver inputPathResolver = new InputPathResolver();
        inputPathResolver.setMinDate(minDate);
        inputPathResolver.setMaxDate(maxDate);
        inputPathResolver.setRegionName(regionName);
        return inputPathResolver.resolveMultiYear(inputPathPatterns);
    }
}
