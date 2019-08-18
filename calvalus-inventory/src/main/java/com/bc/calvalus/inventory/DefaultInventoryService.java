/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.inventory;

import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Default implementation of the {@link InventoryService}.
 *
 * @author MarcoZ
 * @author Norman
 * @author Martin (for ACL)
 */
public class DefaultInventoryService implements InventoryService {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String USER_FILTER = "user=";
    private final AbstractFileSystemService fileSystemService;
    private final boolean withExternalAccessControl;
    private String archiveRootDir = "eodata";

    public DefaultInventoryService(AbstractFileSystemService fileSystemService, String archiveRootDir) {
        this.fileSystemService = fileSystemService;
        this.archiveRootDir = archiveRootDir;
        withExternalAccessControl = Boolean.getBoolean("calvalus.accesscontrol.external");
    }

    @Override
    public ProductSet[] getProductSets(String username, String filter) throws IOException {
        UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(username);
        try {
            return remoteUser.doAs((PrivilegedExceptionAction<ProductSet[]>) () -> {
                FileSystem fileSystem = fileSystemService.getFileSystem(username);
                LOG.fine("DefaultInventoryService user " + username + " fs " + fileSystem + " jcm " + fileSystemService.getJobClientsMap());

                if (filter != null && filter.startsWith(USER_FILTER)) {
                    String filterUserName = filter.substring(USER_FILTER.length());
                    if (filterUserName.equals("all")) {
                        return loadProcessed(fileSystem, "*");
                    } else {
                        return loadProcessed(fileSystem, filterUserName);
                    }
                } else {
                    return loadPredefined(fileSystem);
                }
            });
        } catch (InterruptedException e) {
            throw new IOException("failed to retrieve product sets for user " + username, e);
        }
    }

    private ProductSet[] loadPredefined(FileSystem fileSystem) throws IOException {
        Path databasePath = fileSystemService.makeQualified(fileSystem, archiveRootDir + "/" + ProductSetPersistable.FILENAME + ".new");
        if (!fileSystem.exists(databasePath)) {
            databasePath = fileSystemService.makeQualified(fileSystem, archiveRootDir + "/" + ProductSetPersistable.FILENAME);
        }
        if (fileSystem.exists(databasePath)) {
            final ProductSet[] productSets = readProductSets(fileSystem, new Path[]{databasePath});
            List<ProductSet> accu = new ArrayList<>();
            // check if the datasets are all still available and if the user has access rights
            for (ProductSet productSet : productSets) {
                LOG.fine("loadPredefined " + productSet.getGeoInventory() + " " + productSet.getPath() + " fs " + fileSystem);
                try {
                    if (productSet.getGeoInventory() != null) {
                        for (String item : productSet.getGeoInventory().split(",")) {
                            if (item.startsWith("file:") && !withExternalAccessControl) {
                                LocalFileSystem.newInstance(new Configuration()).exists(new Path(item + "/" + ProductSetPersistable.INDEX));
                            } else if (! item.startsWith("catalogue")) {
                                fileSystem.exists(fileSystemService.makeQualified(fileSystem, item + "/" + ProductSetPersistable.INDEX));
                            }
                        }
                    }
                    if (productSet.getPath() != null && productSet.getPath().length() > 0) {
                        for (String item : productSet.getPath().split(",")) {
                            item = item.replaceAll("\\$", "_");
                            if (item.startsWith("file:") && !withExternalAccessControl) {
                                LocalFileSystem.newInstance(new Configuration()).exists(new Path(item));
                            } else {
                                fileSystem.exists(fileSystemService.makeQualified(fileSystem, item));
                            }
                        }
                    }
                    accu.add(productSet);
                } catch (AccessControlException ignore) {}
            }
            return accu.toArray(new ProductSet[accu.size()]);
        }
        return new ProductSet[0];
    }

    private ProductSet[] loadProcessed(FileSystem fileSystem, String filterUserName) throws IOException {
        final Path[] paths;
        final List<Path> accu = new ArrayList<>();
        final String userDirsPattern = String.format("home/%s", filterUserName);
        final Path userDirsPath = fileSystemService.makeQualified(fileSystem, userDirsPattern);
        final FileStatus[] userDirsStatuses = fileSystem.globStatus(userDirsPath);
        if (userDirsStatuses == null) {
            return new ProductSet[0];
        }
        for (FileStatus userDirStatus : userDirsStatuses) {
            try {
                final FileStatus[] userDatasetsStatus = fileSystem.listStatus(userDirStatus.getPath());
                for (FileStatus userDatasetStatus : userDatasetsStatus) {
                    try {
                        final FileStatus fileStatuses = fileSystem.getFileStatus(new Path(userDatasetStatus.getPath(), ProductSetPersistable.FILENAME));
                        accu.add(fileStatuses.getPath());
                    } catch (AccessControlException | FileNotFoundException ignore) {
                    }
                }
            } catch (AccessControlException | FileNotFoundException ignore) {
            }
        }
        paths = accu.toArray(new Path[accu.size()]);
        return readProductSets(fileSystem, paths);
    }

    private ProductSet[] readProductSets(FileSystem fileSystem, Path[] paths) throws IOException {
        if (paths == null || paths.length == 0) {
            return new ProductSet[0];
        } else {
            List<ProductSet> productSetList = new ArrayList<ProductSet>();
            for (Path path : paths) {
                try {
                    productSetList.addAll(readProductSetFile(fileSystem, path));
                } catch (AccessControlException ignore) {
                } catch (Exception ignore) {
                    LOG.severe("error parsing product set at " + path + ": " + ignore);
                }
            }
            return productSetList.toArray(new ProductSet[0]);
        }
    }

    private List<ProductSet> readProductSetFile(FileSystem fileSystem, Path path) throws IOException {
        return readProductSetFromCsv(fileSystem.open(path));
    }

    static List<ProductSet> readProductSetFromCsv(InputStream is) throws IOException {
        InputStreamReader reader = new InputStreamReader(is);
        try (BufferedReader bufferedReader = new BufferedReader(reader)) {
            List<ProductSet> productSets = new ArrayList<ProductSet>();

            String line = bufferedReader.readLine();
            while (line != null) {
                ProductSet productSet = ProductSetPersistable.convertFromCSV(line);
                if (productSet != null) {
                    productSets.add(productSet);
                }
                line = bufferedReader.readLine();
            }
            return productSets;
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
     *
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
