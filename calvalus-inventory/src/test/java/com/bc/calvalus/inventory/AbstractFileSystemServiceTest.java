/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.inventory.hadoop.CalvalusShFileSystem;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Ignore;
import org.junit.Test;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * @author MarcoZ
 * @author Norman
 */
public class AbstractFileSystemServiceTest {

    String[] PATTERNS1 = new String[] {
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/26/S3A_SL_1_RBT____20190526T152504_20190526T152804_20190526T171540_0180_045_125_2700_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/26/S3A_SL_1_RBT____20190526T151304_20190526T151604_20190526T171031_0179_045_125_1980_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3B_SL_2_LST____20190525T094857_20190525T112956_20190526T161825_6059_025_350______LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T051220_20190525T051520_20190525T073114_0179_045_105_0720_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T050920_20190525T051220_20190526T084420_0179_045_105_0540_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T050920_20190525T051220_20190525T073142_0179_045_105_0540_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T050620_20190525T050920_20190526T084309_0179_045_105_0360_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T050620_20190525T050920_20190525T055302_0180_045_105_0360_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T050320_20190525T050620_20190526T084159_0179_045_105_0180_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T050320_20190525T050620_20190525T055145_0179_045_105_0180_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T050020_20190525T050320_20190526T084048_0179_045_105_0000_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T050020_20190525T050320_20190525T055222_0179_045_105_0000_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T045821_20190525T050020_20190526T083938_0119_045_104_5940_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T045821_20190525T050020_20190525T055225_0119_045_104_5940_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T045521_20190525T045821_20190526T083825_0180_045_104_5760_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T045521_20190525T045821_20190525T055408_0179_045_104_5760_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T045221_20190525T045521_20190526T083713_0179_045_104_5580_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T045221_20190525T045521_20190525T055405_0179_045_104_5580_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T044921_20190525T045221_20190526T083600_0179_045_104_5400_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T044921_20190525T045221_20190525T055408_0179_045_104_5400_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T044621_20190525T044921_20190526T083452_0179_045_104_5220_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T044621_20190525T044921_20190525T055127_0179_045_104_5220_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T044321_20190525T044621_20190526T083345_0179_045_104_5040_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T044321_20190525T044621_20190525T055024_0180_045_104_5040_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T044021_20190525T044321_20190526T083238_0179_045_104_4860_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T044021_20190525T044321_20190525T055240_0179_045_104_4860_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T043721_20190525T044021_20190526T083134_0179_045_104_4680_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T043721_20190525T044021_20190525T055235_0179_045_104_4680_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T043421_20190525T043721_20190526T083033_0179_045_104_4500_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T043421_20190525T043721_20190525T055239_0179_045_104_4500_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T043121_20190525T043421_20190526T082931_0180_045_104_4320_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T043121_20190525T043421_20190525T055327_0179_045_104_4320_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T042821_20190525T043121_20190526T082829_0179_045_104_4140_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T042821_20190525T043121_20190525T055326_0179_045_104_4140_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T042521_20190525T042821_20190526T082723_0179_045_104_3960_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T042521_20190525T042821_20190525T055424_0179_045_104_3960_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T042221_20190525T042521_20190526T082617_0179_045_104_3780_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T042221_20190525T042521_20190525T055428_0180_045_104_3780_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T041921_20190525T042221_20190526T082515_0179_045_104_3600_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T041921_20190525T042221_20190525T055427_0179_045_104_3600_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T041621_20190525T041921_20190526T082412_0179_045_104_3420_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T041621_20190525T041921_20190525T055124_0179_045_104_3420_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T041321_20190525T041621_20190526T082310_0179_045_104_3240_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T041321_20190525T041621_20190525T055049_0179_045_104_3240_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T041021_20190525T041321_20190526T082206_0180_045_104_3060_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T041021_20190525T041321_20190525T055125_0179_045_104_3060_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T040721_20190525T041021_20190526T082103_0179_045_104_2880_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T040721_20190525T041021_20190525T055117_0179_045_104_2880_LN2_O_NR_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T040421_20190525T040721_20190526T081959_0179_045_104_2700_LN2_O_NT_003.SAFE.zip",
    "/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05/25/S3A_SL_1_RBT____20190525T040421_20190525T040721_20190525T055433_0179_045_104_2700_LN2_O_NR_003.SAFE.zip"
    };

    @Test
    public void testGetCommonPathPrefix() throws Exception {
        assertEquals("", AbstractFileSystemService.getCommonPathPrefix(Collections.<String>emptyList()));
        assertEquals("", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("")));
        assertEquals("", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList(".*")));
        assertEquals("", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("abc.*")));
        assertEquals("abc", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("abc")));
        assertEquals("abc", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("abc/.*")));
        assertEquals("abc", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("abc/efg.*")));
        assertEquals("ab/cd", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("ab/cd/MER_.*.N1")));

        assertEquals("ab/cd", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("ab/cd/ef", "ab/cd/gh")));
        assertEquals("ab/cd", AbstractFileSystemService.getCommonPathPrefix(
                Arrays.asList("ab/cd/gh", "ab/cd/ef", "ab/cd/ef/gh")));
        assertEquals("ab/cd", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("ab/cd/.*", "ab/cd/.*")));

        assertEquals("", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("", "bcd")));
        assertEquals("", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("a", "")));
        assertEquals("ab", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList("ab/cd", "ab/ce")));
        assertEquals("/gpfs/DATA3/www/Sentinel3/SLSTR/2019/05", AbstractFileSystemService.getCommonPathPrefix(Arrays.asList(PATTERNS1)));

        /* Talk with MarcoZ about the use of these tests:
        assertEquals("ab", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("abc", "abd")));
        assertEquals("ab", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("abc", "ab")));
        assertEquals("a", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("abc", "acd")));
        assertEquals("", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("abc", "bcd")));
        assertEquals("", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("", "bcd")));
        assertEquals("", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("a", "")));
        assertEquals("ab", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("ab*cd", "ab*cd")));
        assertEquals("ab", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("ab/cd", "ab/ce")));
        assertEquals("ab/cd", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("ab/cd/*", "ab/cd/*")));
        assertEquals("ab/cd", AbstractInventoryService.getCommonPathPrefix(Arrays.asList("ab/cd/*")));
        */
    }

    @Test
    public void testPathIsAbsolute() {
        assertTrue(new Path("/a").isAbsolute());
        assertFalse(new Path("a").isAbsolute());
        assertTrue(new Path("file:///a").isAbsolute());
        assertFalse(new Path("file://a").isAbsolute());
        assertTrue(new Path("hdfs:///a").isAbsolute());
        assertFalse(new Path("hdfs://a").isAbsolute());
    }

    @Test
    public void testGetRegexpForPathGlob() throws Exception {
        // empty
        testGetRegexpForPathGlob("", "^$", "", true);
        testGetRegexpForPathGlob("", "^$", "abc", false);

        // *
        testGetRegexpForPathGlob("*", "^[^/]*$", "", true);
        testGetRegexpForPathGlob("*", "^[^/]*$", "abc", true);
        testGetRegexpForPathGlob("abc/*", "^abc/[^/]*$", "abc", false);
        testGetRegexpForPathGlob("abc/*", "^abc/[^/]*$", "abc/def", true);
        testGetRegexpForPathGlob("abc/*", "^abc/[^/]*$", "abc/def/ghi", false);
        testGetRegexpForPathGlob("abc/*/def", "^abc/[^/]*/def$", "abc/ghi/def", true);
        testGetRegexpForPathGlob("abc/*/def", "^abc/[^/]*/def$", "abc/def/ghi", false);
        testGetRegexpForPathGlob("abc/*/def", "^abc/[^/]*/def$", "abc/def", false);

        // '*' and '.'
        testGetRegexpForPathGlob("abc/*.zip", "^abc/[^/]*\\.zip$", "abc/efg.zip", true);
        testGetRegexpForPathGlob("abc/*.zip", "^abc/[^/]*\\.zip$", "abc/efgzip", false);
        testGetRegexpForPathGlob("abc/*.zip", "^abc/[^/]*\\.zip$", "abc/efg/.zip", false);

        // ?
        testGetRegexpForPathGlob("abc/?.zip", "^abc/[^/]{1}\\.zip$", "abc/a.zip", true);
        testGetRegexpForPathGlob("abc/?.zip", "^abc/[^/]{1}\\.zip$", "abc/2.zip", true);
        testGetRegexpForPathGlob("abc/?.zip", "^abc/[^/]{1}\\.zip$", "abc/.zip", false);
        testGetRegexpForPathGlob("abc/?.zip", "^abc/[^/]{1}\\.zip$", "abc/zip", false);
        testGetRegexpForPathGlob("abc?efg", "^abc[^/]{1}efg$", "abcdefg", true);
        testGetRegexpForPathGlob("abc?efg", "^abc[^/]{1}efg$", "abc.efg", true);
        testGetRegexpForPathGlob("abc?efg", "^abc[^/]{1}efg$", "abc/efg", false);
        testGetRegexpForPathGlob("abc?efg", "^abc[^/]{1}efg$", "abcefg", false);

        // **
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc/def/ghi", true);
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc/def/def/ghi", true);
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc/def/uvw/xyz/ghi", true);
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc/def/uvw/ghi/ghi", true);
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc/ghi", false);
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc", false);
        testGetRegexpForPathGlob("abc/**/ghi", "^abc/.*/ghi$", "abc/", false);
        testGetRegexpForPathGlob("**/def/ghi", "^.*/def/ghi$", "abc/def/ghi", true);
        testGetRegexpForPathGlob("**/def/ghi", "^.*/def/ghi$", "abc/cba/uvw/def/ghi", true);
        testGetRegexpForPathGlob("**/def/ghi", "^.*/def/ghi$", "def/ghi", false);
        testGetRegexpForPathGlob("abc/**", "^abc/.*$", "abc/def", true);
        testGetRegexpForPathGlob("abc/**", "^abc/.*$", "abc/def/ghi", true);
        testGetRegexpForPathGlob("abc/**", "^abc/.*$", "abc", false);
        testGetRegexpForPathGlob("abc/**", "^abc/.*$", "abc/.", true);

        // '**', '*' and '?'
        testGetRegexpForPathGlob("/eodata/**/MER__RR_?P*.N1", "^/eodata/.*/MER__RR_[^/]{1}P[^/]*\\.N1$",
                                 "/eodata/r03/2005/05/21/MER__RR_2PACR200505211025.N1", true);
        testGetRegexpForPathGlob("/eodata/**/MER__RR_?P*.N1", "^/eodata/.*/MER__RR_[^/]{1}P[^/]*\\.N1$",
                                 "/eodata/r03/2005/05/21/MER__RR_2BACR200505211025.N1", false);
        testGetRegexpForPathGlob("/eodata/**/MER__RR_?P*.N1", "^/eodata/.*/MER__RR_[^/]{1}P[^/]*\\.N1$",
                                 "/eodata/MER__RR_2BACR200505211025.N1", false);

    }

    private void testGetRegexpForPathGlob(String testGlob, String expectedRegexp, String testPath,
                                          boolean expectedMatch) {
        String regex = DefaultInventoryService.getRegexpForPathGlob(testGlob);
        assertEquals(expectedRegexp, regex);
        assertEquals(expectedMatch, Pattern.matches(regex, testPath));
    }

    @Test
    public void testPatternMatching() throws Exception {
        String regex = "foo/[^_.].*";
        assertTrue("foo/abc".matches(regex));
        assertFalse("foo/_abc".matches(regex));
        assertFalse("foo/.abc".matches(regex));

        regex = ".*\\.(N1|nc|hdf|seq)$";
        assertTrue("MER.N1".matches(regex));
        assertTrue("MER.hdf".matches(regex));
        assertTrue("MER.nc".matches(regex));
        assertTrue("MER.seq".matches(regex));
        assertTrue("MER.seq".matches(regex));
        assertFalse("MER.seq.index".matches(regex));
    }

    @Ignore
    @Test
    public void testCollectFileStatuses() throws Exception {
        System.setProperty("calvalus.accesscontrol.external", "true");
        String user = "boe";
        JobConf jobConf = new JobConf();
        UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(user);
        FileStatus[] fileStatuses = remoteUser.doAs((PrivilegedExceptionAction<FileStatus[]>) () -> {
            jobConf.set("calvalus.accesscontrol.external", "true");
            JobClientsMap jobClientsMap = new JobClientsMap(jobConf);
            jobClientsMap.getJobClient(user);
            FileSystem fileSystem = jobClientsMap.getFileSystem(user);
            HdfsFileSystemService fsService = new HdfsFileSystemService(jobClientsMap);
            List<String> patterns = new ArrayList<>();
            patterns.add("/calvalus/testdir/t.*");
            return fsService.globFileStatuses(patterns, jobConf);
        });
        assertEquals("number of entries", 2, fileStatuses.length);
    }
}
