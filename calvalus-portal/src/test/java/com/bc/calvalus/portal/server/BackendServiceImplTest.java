package com.bc.calvalus.portal.server;

import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BackendServiceImplTest {

    private static final String originalSamlToken = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><Security xmlns=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd\">  <saml2:Assertion xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" ID=\"ID-1507645739201\" IssueInstant=\"2017-10-10T14:28:59.201Z\" Version=\"2.0\"><saml2:Issuer>http://geotest.caf.dlr.de/eums</saml2:Issuer><ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">" +
            "<ds:SignedInfo>" +
            "<ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/>" +
            "<ds:SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#rsa-sha1\"/>" +
            "<ds:Reference URI=\"#ID-1507645739201\">" +
            "<ds:Transforms>" +
            "<ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/>" +
            "<ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/>" +
            "</ds:Transforms>" +
            "<ds:DigestMethod Algorithm=\"http://www.w3.org/2001/04/xmlenc#sha256\"/>" +
            "<ds:DigestValue>f4O4vs2V+QDiTgKgs8QYhN6mPlVaiJj4ZgR5LZg8Now=</ds:DigestValue>" +
            "</ds:Reference>" +
            "</ds:SignedInfo>" +
            "<ds:SignatureValue>" +
            "KH3dbMGG0/vk4gdEPJpBroAiuV8gwa+8JD3s5jVpLzbOjwruU+KxIWnhQihlKM7yq7AdtkM2146T" +
            "5Ey4ai7/KSM/LSxRIGXzTNqAz9+qvmmD1izxcsUWjPfBVodMMiS7hdIGpE1fXXr6pYtaldeXMu8t" +
            "XAanx73ff9UeVUXx63g=" +
            "</ds:SignatureValue>" +
            "</ds:Signature><saml2:Subject><saml2:NameID>cvuser1</saml2:NameID><saml2:SubjectConfirmation Method=\"urn:oasis:names:tc:SAML:2.0:cm:bearer\"/></saml2:Subject><saml2:Conditions NotBefore=\"2017-10-10T14:28:59.201Z\" NotOnOrAfter=\"2017-10-10T14:33:59.201Z\"/><saml2:AuthnStatement AuthnInstant=\"2017-10-10T14:28:59.201Z\"><saml2:AuthnContext><saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef></saml2:AuthnContext></saml2:AuthnStatement><saml2:AttributeStatement/></saml2:Assertion></Security>";
    
    @Test
    public void testFixRootNode() throws Exception {
        Document document = parseXml(originalSamlToken);
        document = BackendServiceImpl.fixRootNode(document);
        assertNotNull(document);
        assertEquals("saml2:Assertion", document.getFirstChild().getNodeName());
    }

    private static Document parseXml(String xml) throws IOException, ParserConfigurationException, SAXException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputStream inputStream = new ByteArrayInputStream(xml.getBytes());
        return db.parse(inputStream);
    }

}