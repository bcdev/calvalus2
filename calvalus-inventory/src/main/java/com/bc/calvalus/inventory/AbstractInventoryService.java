package com.bc.calvalus.inventory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.io.CsvReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract implementation of the {@link InventoryService}.
 *
 * @author MarcoZ
 * @author Norman
 */
public abstract class AbstractInventoryService implements InventoryService {

    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final DateFormat DATE_FORMAT = ProductData.UTC.createDateFormat(DATE_PATTERN);
    private final FileSystem fileSystem;

    public AbstractInventoryService(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }


    public FileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public ProductSet[] getProductSets(String filter) throws Exception {
        InputStream is = getInputStream("eodata/product-sets.csv");
        try {
            return readProductSetFromCsv(is);
        } catch (Exception e) {
            throw new Exception("Failed to load list of product sets.", e);
        } finally {
            is.close();
        }
    }

    private InputStream getInputStream(String path) throws IOException {
        Path databasePath = new Path(getQualifiedPath(path));
        return getFileSystem().open(databasePath);
    }

    static ProductSet[] readProductSetFromCsv(InputStream is) throws IOException, ParseException {
        InputStreamReader reader = new InputStreamReader(is);
        CsvReader csvReader = new CsvReader(reader, new char[]{';'});
        ArrayList<ProductSet> productSets = new ArrayList<ProductSet>();
        List<String[]> stringRecords = csvReader.readStringRecords();
        for (String[] record : stringRecords) {
            if (record.length == 4) {
                String name = record[0];
                String path = record[1];
                Date date1 = DATE_FORMAT.parse(record[2]);
                Date date2 = DATE_FORMAT.parse(record[3]);
                productSets.add(new ProductSet(null, name, path, date1, date2, null, null));
            }
        }
        return productSets.toArray(new ProductSet[productSets.size()]);
    }

    /**
     * @return An absolute path that is used to make relative paths absolute.
     */
    protected abstract String getContextPath();

    @Override
    public String[] globPaths(List<String> pathPatterns) throws IOException {
        Pattern pattern = createPattern(pathPatterns);
        String commonPathPrefix = getCommonPathPrefix(pathPatterns);
        Path qualifiedPath = makeQualified(commonPathPrefix);
        return collectFiles(qualifiedPath, pattern);
    }

    @Override
    public String getQualifiedPath(String path) {
        return makeQualified(path).toString();
    }

    @Override
    public OutputStream addFile(String path) throws IOException {
        return getFileSystem().create(makeQualified(path)) ;
    }

    @Override
    public boolean removeFile(String path) throws IOException {
        return getFileSystem().delete(makeQualified(path), false);
    }

    private Pattern createPattern(List<String> inputRegexs) {
        if (inputRegexs.size() == 0) {
            return null;
        }
        StringBuilder hugePattern = new StringBuilder(inputRegexs.size() * inputRegexs.get(0).length());
        for (String regex : inputRegexs) {
            Path qualifiedPath = makeQualified(regex);
            hugePattern.append(qualifiedPath.toString());
            hugePattern.append("|");
        }
        hugePattern.setLength(hugePattern.length() - 1);
        return Pattern.compile(hugePattern.toString());
    }

    protected Path makeQualified(String child) {
        Path path = new Path(child);
        if (!path.isAbsolute()) {
            path = new Path(getContextPath(), path);
        }
        return fileSystem.makeQualified(path);
    }

    private String[] collectFiles(Path path, Pattern pattern) throws IOException {
        List<String> result = new ArrayList<String>(1000);
        collectFiles(path, pattern, result);
        return result.toArray(new String[result.size()]);
    }

    private void collectFiles(Path path, Pattern pattern, List<String> result) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        if (fileStatuses != null) {
            Matcher matcher = null;
            if (pattern != null) {
                matcher = pattern.matcher("");
            }
            for (FileStatus fStat : fileStatuses) {
                if (fStat.isDir()) {
                    collectFiles(fStat.getPath(), pattern, result);
                } else {
                    String fPath = fStat.getPath().toString();
                    if (matcher != null) {
                        matcher.reset(fPath);
                        if (matcher.matches()) {
                            result.add(fPath);
                        }
                    } else {
                        result.add(fPath);
                    }
                }
            }
        }
    }

    static String getCommonPathPrefix(List<String> strings) {
        if (strings.size() == 0) {
            return "";
        }
        char firstChar = 0;
        for (int pos = 0; ; pos++) {
            for (int i = 0; i < strings.size(); i++) {
                String string = strings.get(i);
                if (pos == string.length()) {
                    return string;
                }
                if (string.charAt(pos) == '*') {
                    return stripAfterLastSlash(string, pos);
                }
                if (i == 0) {
                    firstChar = string.charAt(pos);
                } else if (string.charAt(pos) != firstChar) {
                    return stripAfterLastSlash(string, pos);
                }
            }
        }
    }

    private static String stripAfterLastSlash(String string, int pos) {
        int slashPos = string.lastIndexOf('/', pos);
        if (slashPos != -1) {
            return string.substring(0, slashPos);
        } else {
            return "";
        }
    }


    /* Talk with MarcoZ about the use of this impl.:
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
    */

    /**
     * Gets the regular expression for the given path containing wildcards.
     * <ol>
     * <li>'?' is used to match any character in a directory/file name (so it does not match the '/' character).</li>
     * <li>'*' is used to match zero or more characters in a directory/file name (so it does not match the '/' character).</li>
     * <li>'**' is used to match zero or more directories (so it matches also the '/' character).</li>
     * </ol>
     *
     * @param wildcardPath The path containing wildcards.
     * @return The corresponding regular expression.
     */
    public static String getRegexpForPathGlob(String wildcardPath) {
        final String regexpMetaCharacters = "([{\\^-$|]})?*+.";
        final String wildcardMetaCharacters = "?*";
        final StringBuilder regexp = new StringBuilder();
        regexp.append('^');  // matches line start
        final int n = wildcardPath.length();
        for (int i = 0; i < n; i++) {
            char c = wildcardPath.charAt(i);
            if (regexpMetaCharacters.indexOf(c) != -1
                    && wildcardMetaCharacters.indexOf(c) == -1) {
                regexp.append('\\');
                regexp.append(c);
            } else if (c == '?') {
                regexp.append("[^/]{1}");
            } else if (c == '*') {
                if (i < n - 1) {
                    if (wildcardPath.charAt(i + 1) == '*') {
                        regexp.append(".*");
                        i++;
                    } else {
                        regexp.append("[^/]*");
                    }
                } else {
                    regexp.append("[^/]*");
                }
            } else {
                regexp.append(c);
            }
        }
        regexp.append('$');  // matches line end
        return regexp.toString();
    }
}
