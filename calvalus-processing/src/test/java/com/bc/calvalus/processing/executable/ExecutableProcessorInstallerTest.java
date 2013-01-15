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

package com.bc.calvalus.processing.executable;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExecutableProcessorInstallerTest {

    @Test
    public void testIsArchive() throws Exception {
        assertTrue(ExecutableProcessorInstaller.isArchive(new Path("myarchie.tgz")));
        assertTrue(ExecutableProcessorInstaller.isArchive(new Path("myarchie.zip")));
        assertTrue(ExecutableProcessorInstaller.isArchive(new Path("myarchie.tar.gz")));
        assertTrue(ExecutableProcessorInstaller.isArchive(new Path("myarchie.tar")));
        assertFalse(ExecutableProcessorInstaller.isArchive(new Path("myarchie.TAR")));
        assertFalse(ExecutableProcessorInstaller.isArchive(new Path("myarchie.sh")));
    }

    @Test
    public void testStripArchiveExtension() throws Exception {
        assertEquals("myarchie", ExecutableProcessorInstaller.stripArchiveExtension("myarchie.tgz"));
        assertEquals("myarchie", ExecutableProcessorInstaller.stripArchiveExtension("myarchie.zip"));
        assertEquals("myarchie", ExecutableProcessorInstaller.stripArchiveExtension("myarchie.tar.gz"));
        assertEquals("myarchie", ExecutableProcessorInstaller.stripArchiveExtension("myarchie.tar"));
        assertEquals(null, ExecutableProcessorInstaller.stripArchiveExtension("myarchie.sh"));
    }
}
