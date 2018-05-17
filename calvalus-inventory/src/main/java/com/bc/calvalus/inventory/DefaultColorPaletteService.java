/*
 * Copyright (C) 2018 Brockmann Consult GmbH (info@brockmann-consult.de)
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
 * Default implementation of the {@link ColorPaletteService}.
 *
 * @author Declan
 */
public class DefaultColorPaletteService implements ColorPaletteService {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String USER_FILTER = "user=";
    private final AbstractFileSystemService fileSystemService;
    private final boolean withExternalAccessControl;
    private String colorPaletteRootDir = "aux";

    public DefaultColorPaletteService(AbstractFileSystemService fileSystemService, String colorPaletteRootDir) {
        this.fileSystemService = fileSystemService;
        this.colorPaletteRootDir = colorPaletteRootDir;
        withExternalAccessControl = Boolean.getBoolean("calvalus.accesscontrol.external");
    }

    @Override
    public ColorPaletteSet[] getColorPaletteSets(String username, String filter) throws IOException {
        UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(username);
        try {
            return remoteUser.doAs((PrivilegedExceptionAction<ColorPaletteSet[]>) () -> {
                FileSystem fileSystem = fileSystemService.getFileSystem(username);
                LOG.fine("DefaultColorPaletteService user " + username + " fs " + fileSystem + " jcm " + fileSystemService.getJobClientsMap());
                return loadPredefined(fileSystem);
            });
        } catch (InterruptedException e) {
            throw new IOException("failed to retrieve color palette sets for user " + username, e);
        }
    }

    private ColorPaletteSet[] loadPredefined(FileSystem fileSystem) throws IOException {
        Path databasePath = fileSystemService.makeQualified(fileSystem, colorPaletteRootDir + "/" + ColorPaletteSetPersistable.FILENAME );
        if (fileSystem.exists(databasePath)) {
            final ColorPaletteSet[] colorPaletteSets = readColorPaletteSets(fileSystem, new Path[]{databasePath});
            return colorPaletteSets;
        }
        return new ColorPaletteSet[0];
    }

    private ColorPaletteSet[] readColorPaletteSets(FileSystem fileSystem, Path[] paths) throws IOException {
        if (paths == null || paths.length == 0) {
            return new ColorPaletteSet[0];
        } else {
            List<ColorPaletteSet> colorPaletteSetList = new ArrayList<ColorPaletteSet>();
            for (Path path : paths) {
                try {
                    colorPaletteSetList.addAll(readColorPaletteSetFile(fileSystem, path));
                } catch (AccessControlException ignore) {}
            }
            return colorPaletteSetList.toArray(new ColorPaletteSet[0]);
        }
    }

    private List<ColorPaletteSet> readColorPaletteSetFile(FileSystem fileSystem, Path path) throws IOException {
        return readColorPaletteSetFromCsv(fileSystem.open(path));
    }

    static List<ColorPaletteSet> readColorPaletteSetFromCsv(InputStream is) throws IOException {
        InputStreamReader reader = new InputStreamReader(is);
        try (BufferedReader bufferedReader = new BufferedReader(reader)) {
            List<ColorPaletteSet> colorPaletteSets = new ArrayList<ColorPaletteSet>();

            String line = bufferedReader.readLine();
            while (line != null) {
                ColorPaletteSet colorPaletteSet= ColorPaletteSetPersistable.convertFromCSV(line);
                if (colorPaletteSet != null) {
                    colorPaletteSets.add(colorPaletteSet);
                }
                line = bufferedReader.readLine();
            }
            return colorPaletteSets;
        }
    }
}
