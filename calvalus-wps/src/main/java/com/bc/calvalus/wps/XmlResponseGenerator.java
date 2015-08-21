package com.bc.calvalus.wps;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by hans on 10/08/2015.
 */
public class XmlResponseGenerator {

    private static final int PORT_NUMBER = 9080;
    private static final String NC_FILE_EXTENSION = ".nc";

    public void constructXmlOutput(XMLStreamWriter xmlStreamWriter, File stagingDirectory, String stagingDirectoryPath)
                throws XMLStreamException, IOException {

        File[] productFiles = stagingDirectory.listFiles();

        xmlStreamWriter.writeStartElement("productionUrls");

        if (productFiles != null) {
            constructProductUrlElements(xmlStreamWriter, stagingDirectoryPath, productFiles);
        }

        xmlStreamWriter.writeEndElement();
        xmlStreamWriter.writeEndDocument();
        xmlStreamWriter.flush();
        xmlStreamWriter.close();
    }

    private void constructProductUrlElements(XMLStreamWriter xmlStreamWriter, String stagingDirectoryPath, File[] productFiles)
                throws XMLStreamException, UnknownHostException {
        for (File productFile : productFiles) {
            if(productFile.getName().endsWith(NC_FILE_EXTENSION)){
                constructSingleProductUrlElement(xmlStreamWriter, stagingDirectoryPath, productFile);
            }
        }
    }

    private void constructSingleProductUrlElement(XMLStreamWriter xmlStreamWriter, String stagingDirectoryPath, File productFile)
                throws XMLStreamException, UnknownHostException {
        xmlStreamWriter.writeStartElement("productionUrl");
        xmlStreamWriter.writeCharacters("http://"
                                        + InetAddress.getLocalHost().getHostName()
                                        + ":" + PORT_NUMBER
                                        + "/" + stagingDirectoryPath
                                        + "/" + productFile.getName());
        xmlStreamWriter.writeEndElement();
    }
}
