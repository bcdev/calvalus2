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

import static com.bc.calvalus.portal.server.BackendServiceImpl.fixRootNode;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BackendServiceImplTest {

    private static final String originalSamlToken =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><Security xmlns=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd\"><saml2:Assertion xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" ID=\"ID-1511536582322\" IssueInstant=\"2017-11-24T15:16:22.322Z\" Version=\"2.0\"><saml2:Issuer>http://geotest.caf.dlr.de/eums</saml2:Issuer><ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">" +
                    "<ds:SignedInfo>" +
                    "<ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/>" +
                    "<ds:SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#rsa-sha1\"/>" +
                    "<ds:Reference URI=\"#ID-1511536582322\">" +
                    "<ds:Transforms>" +
                    "<ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/>" +
                    "<ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"><ec:InclusiveNamespaces xmlns:ec=\"http://www.w3.org/2001/10/xml-exc-c14n#\" PrefixList=\"xsd\"/></ds:Transform>" +
                    "</ds:Transforms>" +
                    "<ds:DigestMethod Algorithm=\"http://www.w3.org/2001/04/xmlenc#sha256\"/>" +
                    "<ds:DigestValue>HOrnvOqRkeVybiLkPk6o4j3QkCgV7TnirmoeEd/v9f0=</ds:DigestValue>" +
                    "</ds:Reference>" +
                    "</ds:SignedInfo>" +
                    "<ds:SignatureValue>" +
                    "PyZyIoTYSEMReR0MR+9bFgIiMh0UyKD3UNLSvTgPg26DwTH3r1euVzM+qfuhx0HRs7DsvBCBWLZl" +
                    "njn9gOyl9b1ITnCnMYgG1ry6y+vhcdw1KQnCGgzjqR06xIsujg45wILC1gV4Dt3zU930WZz2xNo5" +
                    "EhGUgaeacbWxDtBDR8Y=" +
                    "</ds:SignatureValue>" +
                    "</ds:Signature><saml2:Subject><saml2:NameID>cvuser1</saml2:NameID><saml2:SubjectConfirmation Method=\"urn:oasis:names:tc:SAML:2.0:cm:bearer\"/></saml2:Subject><saml2:Conditions NotBefore=\"2017-11-24T15:16:22.322Z\" NotOnOrAfter=\"2017-11-24T15:21:22.322Z\"/><saml2:AuthnStatement AuthnInstant=\"2017-11-24T15:16:22.322Z\"><saml2:AuthnContext><saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef></saml2:AuthnContext></saml2:AuthnStatement><saml2:AttributeStatement><saml2:Attribute Name=\"name\"><saml2:AttributeValue xsi:type=\"xsd:string\">cvuser1</saml2:AttributeValue></saml2:Attribute><saml2:Attribute Name=\"uid\"><saml2:AttributeValue xsi:type=\"xsd:string\">cvuser1</saml2:AttributeValue></saml2:Attribute><saml2:Attribute Name=\"memberOf\"><saml2:AttributeValue xsi:type=\"xsd:string\">cn=cdg_user,ou=CODE-DE-SSO,ou=CODE-DE,dc=codede,dc=dlr,dc=de</saml2:AttributeValue><saml2:AttributeValue xsi:type=\"xsd:string\">cn=cdg_processing,ou=CODE-DE-SSO,ou=CODE-DE,dc=codede,dc=dlr,dc=de</saml2:AttributeValue><saml2:AttributeValue xsi:type=\"xsd:string\">cn=cdg_user_cat1,ou=CODE-DE-SSO,ou=CODE-DE,dc=codede,dc=dlr,dc=de</saml2:AttributeValue></saml2:Attribute><saml2:Attribute Name=\"mail\"><saml2:AttributeValue xsi:type=\"xsd:string\">test@brockmann.de</saml2:AttributeValue></saml2:Attribute></saml2:AttributeStatement></saml2:Assertion></Security>";

    @Test
    public void testFixRootNode() throws Exception {
        Document document = parseXml(originalSamlToken);
        document = fixRootNode(document);
        assertNotNull(document);
        assertEquals("saml2:Assertion", document.getFirstChild().getNodeName());
    }

    @Test
    public void testGetGroupMemberships() throws Exception {
        Document doc = parseXml(originalSamlToken);
        doc = fixRootNode(doc);
        String[] groupMemberships = BackendServiceImpl.getGroupMemberships(doc).toArray(new String[0]);
        assertArrayEquals(new String[]{"cdg_user", "cdg_processing", "cdg_user_cat1"}, groupMemberships);
    }

    private static Document parseXml(String xml) throws IOException, ParserConfigurationException, SAXException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputStream inputStream = new ByteArrayInputStream(xml.getBytes());
        return db.parse(inputStream);
    }

}