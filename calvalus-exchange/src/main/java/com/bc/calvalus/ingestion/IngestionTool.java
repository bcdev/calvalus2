package com.bc.calvalus.ingestion;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * A tool to archive all MER_RR__1P or MER_FRS_1P products found in a given directory
 * into a computed directory in the HDFS archive.
 * <pre>
 * Usage:
 *    hadoop --config ${configDir} jar ${jobJar} com.bc.calvalus.ingestion.IngestionTool ( ${sourceDir} | ${sourceFiles} ) [-producttype=${productType}] [-revision=${revision}] [-replication=${replication}] [-blocksize=${blocksize}]
 * </pre>
 */
public class IngestionTool {

    public static final String DEFAULT_PRODUCT_TYPE = "MER_RR__1P";
    public static final String DEFAULT_REVISION = "r03";
    //static final String DEFAULT_PATTERN = "<type>.*\.N1";

    public static final SimpleDateFormat YEAR_MONTH_DAY_FORMAT = new SimpleDateFormat("yyyy/MM/dd");
    public static final SimpleDateFormat YEAR_DAY_OF_YEAR_FORMAT = new SimpleDateFormat("yyyyDDD");
    public static final SimpleDateFormat YEAR2_DAY_OF_YEAR_FORMAT = new SimpleDateFormat("yyDDD");
    public static final SimpleDateFormat MONTH_DAY_YEAR2_FORMAT = new SimpleDateFormat("MMddyy");

    static {
        YEAR_MONTH_DAY_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        YEAR_DAY_OF_YEAR_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        YEAR2_DAY_OF_YEAR_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        MONTH_DAY_YEAR2_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static final long MINIMUM_BLOCK_SIZE = 1048576;
    public static final long MAXIMUM_BLOCK_SIZE = 2147483648L;

    public static int handleIngestionCommand(CommandLine commandLine, String[] files, FileSystem hdfs) throws IOException {
        String productType = DEFAULT_PRODUCT_TYPE;
        String revision = DEFAULT_REVISION;
        long blockSizeParameter = -1;


        if (commandLine.hasOption("producttype")) {
            productType = commandLine.getOptionValue("producttype");
        }
        if (commandLine.hasOption("revision")) {
            revision = commandLine.getOptionValue("revision");
        }
        if (commandLine.hasOption("blocksize")) {
            blockSizeParameter = Long.parseLong(commandLine.getOptionValue("blocksize"));
        }

        final String filenamePattern;
        if (commandLine.hasOption("filenamepattern")) {
            filenamePattern = commandLine.getOptionValue("filenamepattern");
        } else {
            filenamePattern = productType + ".*\\.N1";
        }
        Pattern pattern = Pattern.compile(filenamePattern);
        final String timeElements;
        if (commandLine.hasOption("timeelements")) {
            timeElements = commandLine.getOptionValue("timeelements");
        } else {
            timeElements = null;
        }
        final String timeFormat;
        if (commandLine.hasOption("timeformat")) {
            timeFormat = commandLine.getOptionValue("timeformat");
        } else {
            timeFormat = null;
        }
        final String pathTemplate;
        if (commandLine.hasOption("pathtemplate")) {
            pathTemplate = commandLine.getOptionValue("pathtemplate");
        } else {
            pathTemplate = null;
        }

        final short replication;
        if (commandLine.hasOption("replication")) {
            replication = Short.parseShort(commandLine.getOptionValue("replication"));
        } else {
            replication = hdfs.getDefaultReplication();
        }

        boolean verify = commandLine.hasOption("verify");

        // determine input files
        List<IngestionFile> ingestionFiles = new ArrayList<IngestionFile>();
        for (String path : files) {
            File file = new File(path);
            collectInputFiles(path, file, pattern, timeElements, timeFormat, productType, revision, pathTemplate, ingestionFiles);
        }
        if (ingestionFiles.isEmpty()) {
            throw new FileNotFoundException("no files found");
        }
        System.out.format("%d files to be ingested\n", ingestionFiles.size());


        return ingest(productType, revision, blockSizeParameter, pattern, hdfs, replication, verify, ingestionFiles);
    }

    static class IngestionFile {
        IngestionFile(File input, String output) {
            this.input = input;
            this.output = output;
        }
        File input;
        String output;
    }

    private static int ingest(String productType, String revision, long blockSizeParameter, Pattern pattern, FileSystem hdfs, short replication, boolean verify, List<IngestionFile> sourceFiles) throws IOException {
        // cache HDFS parameters for block size
        final int bufferSize = hdfs.getConf().getInt("io.file.buffer.size", 4096);
        final int checksumSize = hdfs.getConf().getInt("io.bytes.per.checksum", 512);

        // loop over input files
        for (IngestionFile sourceFile : sourceFiles) {
            //String archivePath = getArchivePath(sourceFile, productType, revision, pattern);
            final String archivePath = sourceFile.output;

            // calculate block size to cover complete N1
            // blocksize must be a multiple of checksum size
            long fileSize = sourceFile.input.length();
            long blockSize;
            if (blockSizeParameter == -1) {
                blockSize = ((fileSize + checksumSize - 1) / checksumSize) * checksumSize;
                if (blockSize < MINIMUM_BLOCK_SIZE) {
                    blockSize = MINIMUM_BLOCK_SIZE;
                } else if (blockSize > MAXIMUM_BLOCK_SIZE) {
                    blockSize = MAXIMUM_BLOCK_SIZE;
                }
            } else {
                blockSize = ((blockSizeParameter + checksumSize - 1) / checksumSize) * checksumSize;
            }

            // construct HDFS output stream
            Path destPath = new Path(archivePath, sourceFile.input.getName());
            // copy if either verification is off or target does not exist or target has different size
            if (! verify || ! hdfs.exists(destPath) || hdfs.listStatus(destPath) == null || hdfs.listStatus(destPath)[0].getLen() < fileSize) {
                int attempt = 1;
                boolean finished = false;
                IOException exception = null;
                System.out.println(MessageFormat.format("archiving {0} in {1}", sourceFile, archivePath));
                while (attempt <= 3 && !finished) {
                    short actualReplication = attempt == 1 ? replication : 3;
                    OutputStream out = hdfs.create(destPath, true, bufferSize, actualReplication, blockSize);
                    FileInputStream in = new FileInputStream(sourceFile.input);
                    try  {
                        IOUtils.copyBytes(in, out, hdfs.getConf(), true);
                        finished = true;
                        if (actualReplication != replication) {
                            hdfs.setReplication(destPath, replication);
                        }
                    }catch (IOException ioe){
                        System.err.print("copying attempt " + attempt + " failed.");
                        ioe.printStackTrace();
                        exception = ioe;
                    } finally {
                        out.close();
                        in.close();
                    }
                    attempt++;
                }
                if (!finished) {
                    throw new IOException("Failed to copy: " + sourceFile, exception);
                }
            } else {
                System.out.println(MessageFormat.format("skipping {0} existing in {1}", sourceFile, archivePath));
            }
        }

        return 0;
    }

    private static void collectInputFiles(String rootDir, File file, Pattern pattern, String timeElements, String timeFormat, String productType, String revision, String pathTemplate, List<IngestionFile> accu) throws IOException {
        // handle recursive directory search
        if (file.isDirectory()) {
            if ("lost+found".equals(file.getName())) {
                return;
            }
            final File[] files = file.listFiles();
            if (files == null) {
                throw new FileNotFoundException("cannot access directory " + file.getPath());
            }
            for (File f : files) {
                collectInputFiles(rootDir, f, pattern, timeElements, timeFormat, productType, revision, pathTemplate, accu);
            }
        // handle non-existent named file
        } else if (! file.isFile()) {
            throw new FileNotFoundException(file.getAbsolutePath());
        }
        // handle file found ...
        final String path = formatPath(rootDir, file, pattern, timeElements, timeFormat, productType, revision, pathTemplate);
        if (path != null) {
            accu.add(new IngestionFile(file, path));
        }
    }

    static String formatPath(String rootDir, File file, Pattern pattern, String timeElements, String timeFormat, String productType, String revision, String pathTemplate) throws IOException {
        final String relPath;
        if (pattern.pattern().contains("/")) {
            relPath = file.getAbsolutePath().substring(rootDir.length()+1);
        } else {
            relPath = file.getName();
        }
        final Matcher matcher = pattern.matcher(relPath);
        if (! matcher.matches()) {
            return null;
        } else if (pathTemplate != null) {
            String path = pathTemplate;
            if (timeElements != null && timeFormat != null) {
                try {
                    final String timeString = expand(matcher, timeElements);
                    final Date date = new SimpleDateFormat(timeFormat).parse(timeString);
                    path = new SimpleDateFormat(path).format(date);
                } catch (ParseException e) {
                    throw new IOException("parsing of date in " + file.getName() + " using " + pattern.pattern() + " " + timeElements + " " + timeFormat + " failed", e);
                }
            }
            return expand(matcher, path);
        } else {
            final String subPath = getDatePath(file, productType, pattern);
            return String.format("/calvalus/eodata/%s/%s/%s", productType, revision, subPath);
        }
    }

    public static String expand(Matcher source, String template) throws IndexOutOfBoundsException, IllegalStateException {
        Pattern elementPattern = Pattern.compile("\\\\(\\d+)");
        Matcher elementMatcher = elementPattern.matcher(template);
        StringBuffer result = new StringBuffer();
        try {
            while (elementMatcher.find()) {
                int groupIdx = Integer.parseInt(elementMatcher.group(1));
                elementMatcher.appendReplacement(result, source.group(groupIdx));
            }
            elementMatcher.appendTail(result);
        } catch (NumberFormatException e) {
            throw new IndexOutOfBoundsException(e.getMessage());
        }
        return result.toString();
    }

//    static void collectInputFiles(File file, Pattern filter, List<File> accu) throws IOException {
//        if (file.isDirectory()) {
//            if ("lost+found".equals(file.getName())) {
//                return;
//            }
//            final File[] files = file.listFiles();
//            if (files == null) {
//                throw new FileNotFoundException("cannot access directory " + file.getPath() + ".");
//            }
//            for (File f : files) {
//                collectInputFiles(f, filter, accu);
//            }
//        } else if (file.isFile()) {
//            if (filter.matcher(file.getName()).matches()) {
//                accu.add(file);
//            }
//        } else {
//            throw new FileNotFoundException(file.getAbsolutePath());
//        }
//    }

    /**
     * Implements the archiving "rule"
     *
     *
     * @param sourceFile a file name
     * @param productType the product type
     * @param revision the revision
     * @param pattern
     * @return   an archive path
     */
    static String getArchivePath(File sourceFile, String productType, String revision, Pattern pattern) {
        String subPath = getDatePath(sourceFile, productType, pattern);
        return String.format("/calvalus/eodata/%s/%s/%s", productType, revision, subPath);
    }

    /**
     * Parses file name, determines acquisition date, and constructs subdirectoy path year/month/day,
     * either using the provided pattern if it contains groups or a product type specific pattern
     * if the product type is known
     * <pre>
     *  MER_RR__1PRACR20060530_130506_000026432048_00110_22208_0000.N1
     *  NSS.GHRR.NM.D06365.S1409.E1554.B2348788.WI.gz
     *  A2012280012500.L1A_LAC.bz2
     *  V2KRNP____20070501F083.ZIP
     * </pre>
     *
     * @param sourceFile     file name that contains the concrete date in some encoding
     * @param productType  product type for default pattern selection
     * @param pattern      pattern used if it contains groups in parenthesis for year, month and day or for year and day of year
     * @return             directory path year/mont/day, "." if neither type is known nor pattern contains groups
     * @throws IllegalArgumentException  if pattern does not match
     */
    static String getDatePath(File sourceFile, String productType, Pattern pattern) throws IllegalArgumentException {
        int numberOfParenthesis = countChars(pattern.pattern(), '(');
        if (numberOfParenthesis >= 2) {
            Matcher matcher = pattern.matcher(sourceFile.getName());
            if (! matcher.matches()) {
                throw new IllegalArgumentException("pattern " + pattern.pattern() + " does not match file name " + sourceFile.getName());
            }
            if (matcher.groupCount() == 3) {
                return String.format("%s/%s/%s", matcher.group(1), matcher.group(2), matcher.group(3));
            } else if (matcher.groupCount() == 2 && matcher.group(1).length() == 4 && matcher.group(2).length() == 3) {
                Date date = null;
                try {
                    date = YEAR_DAY_OF_YEAR_FORMAT.parse(matcher.group(1) + matcher.group(2));
                } catch (ParseException e) {
                    throw new IllegalArgumentException("file name " + sourceFile.getName() + " does not contain expected year and dayofyear according to pattern " + pattern.pattern() + ": " + e.getMessage());
                }
                return YEAR_MONTH_DAY_FORMAT.format(date);
            } else if (matcher.groupCount() == 2 && matcher.group(1).length() == 2 && matcher.group(2).length() == 3) {
                Date date = null;
                try {
                    date = YEAR2_DAY_OF_YEAR_FORMAT.parse(matcher.group(1) + matcher.group(2));
                } catch (ParseException e) {
                    throw new IllegalArgumentException("file name " + sourceFile.getName() + " does not contain expected year and dayofyear according to pattern " + pattern.pattern() + ": " + e.getMessage());
                }
                return YEAR_MONTH_DAY_FORMAT.format(date);
            } else {
                throw new IllegalArgumentException("pattern " + pattern.pattern() + " does not contain recognised date in file name " + sourceFile.getName());
            }
        } else if (productType != null && productType.startsWith("MODIS")) {
            try {
                return YEAR_MONTH_DAY_FORMAT.format(YEAR_DAY_OF_YEAR_FORMAT.parse(sourceFile.getName().substring(1, 8)));
            } catch (ParseException e) {
                throw new IllegalArgumentException("file name " + sourceFile + " does not contain recognised date for MODIS default pattern");
            }
        } else if (productType != null && productType.startsWith("SPOT_VGT")) {
            try {
                return YEAR_MONTH_DAY_FORMAT.format(YEAR_DAY_OF_YEAR_FORMAT.parse(sourceFile.getName().substring(10, 18)));
            } catch (ParseException e) {
                throw new IllegalArgumentException("file name " + sourceFile.getName() + " does not contain recognised date for SPOT_VGT default pattern");
            }
        } else if (productType != null && productType.startsWith("AVHRR")) {
            try {
                return YEAR_MONTH_DAY_FORMAT.format(MONTH_DAY_YEAR2_FORMAT.parse(sourceFile.getName().substring(4, 10)));
            } catch (ParseException e) {
                throw new IllegalArgumentException("file name " + sourceFile.getName() + " does not contain recognised date for NOAA AVHRR pattern");
            }
        } else if ("MER_RR__1P".equals(productType) || "MER_FRS_1P".equals(productType)) {
            return String.format("%s/%s/%s",
                                 sourceFile.getName().substring(14, 18),
                                 sourceFile.getName().substring(18, 20),
                                 sourceFile.getName().substring(20, 22));
        } else {
            File dayDir = sourceFile.getParentFile();
            if (dayDir == null || dayDir.getName().length() != 2) {
                return ".";
            }
            File monthDir = dayDir.getParentFile();
            if (monthDir == null || monthDir.getName().length() != 2) {
                return ".";
            }
            File yearDir = monthDir.getParentFile();
            if (yearDir == null || yearDir.getName().length() != 4) {
                return ".";
            }
            return String.format("%s/%s/%s", yearDir.getName(), monthDir.getName(), dayDir.getName());
        }
    }

    private static int countChars(String s, char c) {
        int count = 0;
        for (char i : s.toCharArray()) {
            if (i == c) {
                ++count;
            }
        }
        return count;
    }

//    static class ProductFilenameFilter implements FilenameFilter {
//        private final String productType;
//
//        public ProductFilenameFilter(String productType) {
//            this.productType = productType;
//        }
//
//        @Override
//        public boolean accept(File file, String s) {
//            return file.isDirectory() || (s.startsWith(productType) && s.endsWith(".N1"));
//        }
//    }
}
