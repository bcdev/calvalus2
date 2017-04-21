package com.bc.calvalus.urban.ws;

import com.bc.wps.utilities.PropertiesWrapper;
import com.jcraft.jsch.JSchException;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Optional;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author muhammad.bc.
 */
class CopyReportFileTest {
    @BeforeAll
    static void setUp() throws IOException {
        PropertiesWrapper.loadConfigFile("urbanTEP.properties");
    }

    @Test
    void copyRemoteFile() throws IOException, JSchException {
        CopyReportFile copyReportFile = new CopyReportFile();
        Optional<BufferedReader> bufferedReaderOptional = copyReportFile.readFile("2017-04");
        Assert.assertTrue(bufferedReaderOptional.isPresent());
        WpsService wpsService = new WpsService();
        wpsService.getReportLog(bufferedReaderOptional.get());

    }
}