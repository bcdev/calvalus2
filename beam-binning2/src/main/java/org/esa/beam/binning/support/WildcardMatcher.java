package org.esa.beam.binning.support;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Norman Fomferra
 */
public class WildcardMatcher {

    private static final boolean windowsOs = System.getProperty("os.name").contains("Win");
    private final Pattern pattern;

    public WildcardMatcher(String wildcard) {
        pattern = Pattern.compile(wildcardToRegexp(wildcard));
    }

    static boolean isWindowsOs() {
        return windowsOs;
    }

    public static File[] glob(String filePattern) throws IOException {
        WildcardMatcher matcher = new WildcardMatcher(filePattern);
        File dir = new File(getBasePath(filePattern)).getCanonicalFile();
        ArrayList<File> matchedFiles = new ArrayList<File>();
        collectFiles(matcher, dir, matchedFiles);
        return matchedFiles.toArray(new File[matchedFiles.size()]);
    }

    private static void collectFiles(WildcardMatcher matcher, File dir, List<File> matchedFiles) throws IOException {
        File[] files = dir.listFiles();
        if (files == null) {
            throw new IOException(String.format("Failed to access directory '%s'", dir));
        }
        for (File file : files) {
            if (matcher.matches(file.getPath())) {
                matchedFiles.add(file);
            } else if (file.isDirectory()) {
                collectFiles(matcher, file, matchedFiles);
            }
        }
    }

    static String getBasePath(String filePattern) {
        if (isWindowsOs()) {
            filePattern = filePattern.replace("\\", "/");
        }
        String basePath = filePattern.startsWith("/") ? "/" : "";
        String[] parts = filePattern.split("/");
        for (int i = 0; i < parts.length && !parts[i].equals("**") && !parts[i].contains("*") && !parts[i].contains("?"); i++) {
            if (!parts[i].isEmpty()) {
                basePath += parts[i];
                if (i < parts.length - 1) {
                    basePath += "/";
                }
            }
        }
        return basePath;
    }

    String getRegex() {
        return pattern.pattern();
    }

    String wildcardToRegexp(String wildcard) {

        String s = wildcard;

        if (windowsOs) {
            s = normaliseWindowsPath(s);
        }

        s = s.replace("/**/", "_%SLASHSTARSTARSLASH%_");
        s = s.replace("/**", "_%SLASHSTARSTAR%_");
        s = s.replace("**/", "_%STARSTARSLASH%_");
        s = s.replace("*", "_%STAR%_");
        s = s.replace("?", "_%QUOTE%_");

        String[] metas = new String[]{"\\", "|", "^", "$", "+", ".", "(", ")", "{", "}", "<", ">"};
        for (String meta : metas) {
            s = s.replace(meta, "\\" + meta);
        }

        s = s.replace("_%SLASHSTARSTARSLASH%_", "((/.*/)?|/)");
        s = s.replace("_%SLASHSTARSTAR%_", "(/.*)?");
        s = s.replace("_%STARSTARSLASH%_", "(.*/)?");
        s = s.replace("_%STAR%_", "[^/:]*");
        s = s.replace("_%QUOTE%_", ".");

        return s;
    }

    private String normaliseWindowsPath(String s) {
        return s.toLowerCase().replace("\\", "/");
    }

    public boolean matches(String text) {
        if (windowsOs) {
            text = normaliseWindowsPath(text);
        }
        return pattern.matcher(text).matches();
    }

}
