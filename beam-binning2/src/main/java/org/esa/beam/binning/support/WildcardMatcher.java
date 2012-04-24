package org.esa.beam.binning.support;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
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
        Set<File> fileSet = new TreeSet<File>();
        glob(filePattern, fileSet);
        return fileSet.toArray(new File[fileSet.size()]);
    }

    public static void glob(String filePattern, Set<File> fileSet) throws IOException {
        WildcardMatcher matcher = new WildcardMatcher(filePattern);
        String basePath = getBasePath(filePattern);
        File dir = new File(basePath).getCanonicalFile();
        int validPos = dir.getPath().indexOf(basePath);
        collectFiles(matcher, validPos, dir, fileSet);
    }

    private static void collectFiles(WildcardMatcher matcher, int validPos, File dir, Set<File> fileSet) throws IOException {
        File[] files = dir.listFiles();
        if (files == null) {
            throw new IOException(String.format("Failed to access directory '%s'", dir));
        }
        for (File file : files) {
            String text;
            if (validPos > 0) {
                text = file.getPath().substring(validPos);
            } else {
                text = file.getPath();
            }
            if (matcher.matches(text)) {
                fileSet.add(file);
            } else if (file.isDirectory()) {
                collectFiles(matcher, validPos, file, fileSet);
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
        return new File(basePath).getPath();
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
