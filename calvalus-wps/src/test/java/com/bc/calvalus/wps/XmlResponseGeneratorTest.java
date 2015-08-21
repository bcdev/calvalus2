package com.bc.calvalus.wps;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.*;

import org.junit.*;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.InetAddress;

/**
 * Created by hans on 10/08/2015.
 */
public class XmlResponseGeneratorTest {

    private String hostName;

    /**
     * Class under test.
     */
    private XmlResponseGenerator xmlResponseGenerator;

    @Before
    public void setUp() throws Exception {
        hostName = InetAddress.getLocalHost().getHostName();
    }

    @Test
    public void testConstructXmlOutputWithSingleNcFile() throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        XMLStreamWriter xmlStreamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(byteArrayOutputStream);
        File[] mockFileArray = createFileArrayWithOnlySingleNcFile();
        File mockStagingDirectory = mock(File.class);
        when(mockStagingDirectory.listFiles()).thenReturn(mockFileArray);

        xmlResponseGenerator = new XmlResponseGenerator();
        xmlResponseGenerator.constructXmlOutput(xmlStreamWriter, mockStagingDirectory, "staging-test");

        assertThat(byteArrayOutputStream.toString(), equalTo("<productionUrls>"
                                                             + "<productionUrl>http://" + hostName + ":9080/staging-test/file1.nc</productionUrl>"
                                                             + "</productionUrls>"));
    }

    @Test
    public void testConstructXmlOutputWithEmptyStagingDirectory() throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        XMLStreamWriter xmlStreamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(byteArrayOutputStream);
        File[] mockFileArray = createFileArrayWithOnlySingleNcFile();
        File mockStagingDirectory = mock(File.class);
        when(mockStagingDirectory.listFiles()).thenReturn(mockFileArray);

        xmlResponseGenerator = new XmlResponseGenerator();
        xmlResponseGenerator.constructXmlOutput(xmlStreamWriter, mockStagingDirectory, "");

        assertThat(byteArrayOutputStream.toString(), equalTo("<productionUrls>"
                                                             + "<productionUrl>http://" + hostName + ":9080//file1.nc</productionUrl>"
                                                             + "</productionUrls>"));
    }

    @Test
    public void testConstructXmlOutputWithMultipleNcFiles() throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        XMLStreamWriter xmlStreamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(byteArrayOutputStream);
        File[] mockFileArray = createFileArrayWithMultipleNcFiles();
        File mockStagingDirectory = mock(File.class);
        when(mockStagingDirectory.listFiles()).thenReturn(mockFileArray);

        xmlResponseGenerator = new XmlResponseGenerator();
        xmlResponseGenerator.constructXmlOutput(xmlStreamWriter, mockStagingDirectory, "staging-test");

        assertThat(byteArrayOutputStream.toString(), equalTo("<productionUrls>"
                                                             + "<productionUrl>http://" + hostName + ":9080/staging-test/file1.nc</productionUrl>"
                                                             + "<productionUrl>http://" + hostName + ":9080/staging-test/file2.nc</productionUrl>"
                                                             + "</productionUrls>"));
    }

    @Test
    public void testConstructXmlOutputWithEmptyFileList() throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        XMLStreamWriter xmlStreamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(byteArrayOutputStream);
        File[] mockFileArray = {};
        File mockStagingDirectory = mock(File.class);
        when(mockStagingDirectory.listFiles()).thenReturn(mockFileArray);

        xmlResponseGenerator = new XmlResponseGenerator();
        xmlResponseGenerator.constructXmlOutput(xmlStreamWriter, mockStagingDirectory, "staging-test");

        assertThat(byteArrayOutputStream.toString(), equalTo("<productionUrls/>"));
    }

    @Test
    public void testConstructXmlOutputWithNullFileList() throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        XMLStreamWriter xmlStreamWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(byteArrayOutputStream);
        File mockStagingDirectory = mock(File.class);
        when(mockStagingDirectory.listFiles()).thenReturn(null);

        xmlResponseGenerator = new XmlResponseGenerator();
        xmlResponseGenerator.constructXmlOutput(xmlStreamWriter, mockStagingDirectory, "staging-test");

        assertThat(byteArrayOutputStream.toString(), equalTo("<productionUrls/>"));
    }

    private File[] createFileArrayWithMultipleNcFiles() {
        File mockFile1 = createMockFile("file1.nc");
        File mockFile2 = createMockFile("file2.nc");
        return new File[]{mockFile1, mockFile2};
    }

    private File[] createFileArrayWithOnlySingleNcFile() {
        File mockFile1 = createMockFile("file1.nc");
        File mockFile2 = createMockFile("file2.ignore");
        File mockFile3 = createMockFile("file3.ignore");
        return new File[]{mockFile1, mockFile2, mockFile3};
    }

    private File createMockFile(String fileName) {
        File file = mock(File.class);
        when(file.getName()).thenReturn(fileName);
        return file;
    }
}