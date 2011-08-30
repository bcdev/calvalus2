package com.bc.calvalus.inventory.hadoop;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.io.CsvReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HadoopInventoryService implements InventoryService {

    public static final String CALVALUS_INPUT_PATH = "/calvalus/eodata";
    public static final String CALVALUS_OUTPUTS_PATH = "/calvalus/outputs";

    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final DateFormat DATE_FORMAT = ProductData.UTC.createDateFormat(DATE_PATTERN);

    private final FileSystem fileSystem;

    public HadoopInventoryService(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public ProductSet[] getProductSets(String filter) throws Exception {
        Path databasePath = new Path(CALVALUS_INPUT_PATH, "product-sets.csv");
        try {
            ArrayList<ProductSet> productSets = new ArrayList<ProductSet>();
            Path path1 = fileSystem.makeQualified(databasePath);
            InputStream is = fileSystem.open(path1);
            InputStreamReader reader = new InputStreamReader(is);
            CsvReader csvReader = new CsvReader(reader, new char[]{','});
            List<String[]> stringRecords = csvReader.readStringRecords();
            for (String[] record : stringRecords) {
                if (record.length == 3) {
                    String path = record[0];
                    Date date1 = DATE_FORMAT.parse(record[1]);
                    Date date2 = DATE_FORMAT.parse(record[2]);
                    productSets.add(new ProductSet(path, date1, date2));
                }
            }
            return productSets.toArray(new ProductSet[productSets.size()]);
        } catch (Exception e) {
            throw new Exception("Failed to load list of product sets from '" + databasePath + "'.", e);
        }
    }

    @Override
    public String[] getDataInputPaths(List<String> inputRegexs) throws IOException {
        Pattern pattern = createPattern(inputRegexs);
        String commonPathPrefix = getCommonPathPrefix(inputRegexs);
        Path qualifiedPath = makeQualified(CALVALUS_INPUT_PATH, commonPathPrefix);
        String[] strings = listFilesRecursively(qualifiedPath, pattern);
        return strings;
    }

    @Override
    public String getDataOutputPath(String outputPath) {
        return makeQualified(CALVALUS_OUTPUTS_PATH, outputPath).toString();
    }

    private Pattern createPattern(List<String> inputRegexs) {
        if (inputRegexs.size() == 0) {
            return null;
        }
        StringBuilder hugePattern = new StringBuilder(inputRegexs.size() * inputRegexs.get(0).length());
        for (String regex : inputRegexs) {
            Path qualifiedPath = makeQualified(CALVALUS_INPUT_PATH, regex);
            hugePattern.append(qualifiedPath.toString());
            hugePattern.append("|");
        }
        hugePattern.setLength(hugePattern.length() - 1);
        return Pattern.compile(hugePattern.toString());
    }

    private Path makeQualified(String parent, String child) {
        Path path = new Path(child);
        if (!path.isAbsolute()) {
            path = new Path(parent, path);
        }
        return fileSystem.makeQualified(path);
    }

    private String[] listFilesRecursively(Path path, Pattern pattern) throws IOException {
        List<String> result = new ArrayList<String>(1000);
        listFilesRecursively(result, path, pattern);
        return result.toArray(new String[result.size()]);
    }

    private void listFilesRecursively(List<String> result, Path path, Pattern pattern) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        if (fileStatuses != null) {
            Matcher matcher = null;
            if (pattern != null) {
                matcher = pattern.matcher("");
            }
            for (FileStatus fStat : fileStatuses) {
                if (fStat.isDir()) {
                    listFilesRecursively(result, fStat.getPath(), pattern);
                } else {
                    String fPath = fStat.getPath().toString();
                    if (matcher != null) {
                        matcher.reset(fPath);
                        if(matcher.matches()) {
                            result.add(fPath);
                        }
                    } else {
                        result.add(fPath);
                    }
                }
            }
        }
    }

    static String getCommonPathPrefix(List<String> stringList) {
        if (stringList.size() == 0) {
            return "";
        } else if (stringList.size() == 1) {
            return stringList.get(0);
        }
        String first = stringList.get(0);
        if (first.length() == 0) {
            return "";
        }
        int lastSlash = -1;
        for (int charIndex = 0; charIndex < first.length(); charIndex++) {
            char current = first.charAt(charIndex);
            if (current == '*') {
                return first.substring(0, lastSlash != -1 ? lastSlash : charIndex);
            } else if (current == '/') {
                lastSlash = charIndex;
            }
            for (String s : stringList) {
                if (s.length() <= charIndex || s.charAt(charIndex) != current) {
                    return first.substring(0, lastSlash != -1 ? lastSlash : charIndex);
                }
            }
        }
        return "";
    }
}
