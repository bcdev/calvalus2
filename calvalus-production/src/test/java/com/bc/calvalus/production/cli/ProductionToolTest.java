package com.bc.calvalus.production.cli;

import org.apache.xml.security.Init;
import org.apache.xml.security.encryption.XMLCipher;
import org.apache.xml.security.utils.EncryptionConstants;
import org.junit.Ignore;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test documents how to read a private key and, using it, decipher a ciphered SAML token in Java
 */
public class ProductionToolTest {

    private static String encryptedSaml = "<Security xmlns=\"http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd\">" +
            "  <xenc:EncryptedData xmlns:xenc=\"http://www.w3.org/2001/04/xmlenc#\" Type=\"http://www.w3.org/2001/04/xmlenc#Content\">" +
            "    <xenc:EncryptionMethod Algorithm=\"http://www.w3.org/2001/04/xmlenc#aes128-cbc\"/>" +
            "    <ds:KeyInfo xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">" +
            "<xenc:EncryptedKey>" +
            "        <xenc:EncryptionMethod Algorithm=\"http://www.w3.org/2001/04/xmlenc#rsa-1_5\"/>" +
            "        <xenc:CipherData>" +
            "          <xenc:CipherValue>FsgflNDHljnMKESgBF8JFMUVXN4LtnDB90hguR+C9U0vRIJgsWO6prk7jdexNFOJbfbxpn7pMVA2" +
            "QY3tkTgZLlLpkvWgM0X7M4tvl/J8OlVkNGS1H8z0L6clwP2LyckI2+1MHU3U/Cys2+AVExQOfg8A" +
            "JOHwNdDHvaZ9uiY4vCnffVe4fhnW/c9T23RprWzbaR6+HcsdXEt2unODDsHTXzvUarUckvmkdnlq" +
            "MUCUhALTT9f5K1/gNfUM9jFT42Ic1ft60rzkIIkJGh1MnvOgeJWW9NNnQtqy11XR1ArHpiIQTeYD" +
            "5vmuMN3HDJlEAfg1dy84XMVAANmMZv4nKWMZOw==</xenc:CipherValue>" +
            "        </xenc:CipherData>" +
            "      </xenc:EncryptedKey>" +
            "    </ds:KeyInfo>" +
            "    <xenc:CipherData>" +
            "      <xenc:CipherValue>ZOjpWB3DcKmfWbSI6jenrNrbiN5KdWIfij4yWaBaPRdUdoLQOVz/SF5BDsVnEZXsmgnpJY1csO1m" +
            "4JwbMT1LhzG7UM7qnd1vhVxUgibJzZRr92MXYqhMUEi5S4h4M0YFiPHIh+Av8wNbP4yAjP/J/yKo" +
            "2rt5hzP5PW2Q2OH8ys8+A2TPCO2Cmj6d3jJ2ZFM7J6hS4ItU5ck49zIeJR2wFCAq2tNnB/CgwWNG" +
            "Z3B/3ldl8ye92biAgRCEfV9fO+WRKAH2oj5cdO+O10dCIuM3v0rCpNqRlb9L3NuaRftakVXx5S/W" +
            "NQO6iQY9f1ih7auPsQmLjwdYHQslE2g2bPl6RnW1y/WVkZzJ3GOJIrcVj1dZ5Ea2eZ4D3saGpUfF" +
            "rgz0OkXIXYLtA8204ovTirTd4YotmO5fNlPH99Xpe9cm3k4GegCpQZgqE+8CJfIyGaR6B2vP0MWA" +
            "Ngq/MX3frNaDHHpyFiSzSkrEnejsFqXawmsy+XI60Hiqjp3QRn1yh80ZfkkZRJ/1+6XRsN6Uu8Q9" +
            "I+MB0r43S4dCeqiIAeJuMaJH80kz97bDsQos9xJ2ihGQPcnOl/1ohZ+aTId8zrD0f3d+E0HW3PKu" +
            "du3gEL7jd51r2My/1xcgQOWQJhPjsY6sUBVsY/4DycdiiPpn7IJjerpFC88UA62y7cp3GcQmuoes" +
            "owsD4AVZPVLAw/yU7p+FUgLunYbR2Ru8Wq462+YYIN9F4Hx7UidYgOnb/6RRrkMd8wTDH5Uppc5q" +
            "H7EzTnxl1X4dtOyj68+Dw8PlJo861J3WHtx8AtoT7LldrJOBb/Q7qct+rkLhmhbXpvMlIOlACG5b" +
            "EvP+5x1+quQ7u8l1veCouoxEhEV3/fKopT28Tmob7JAFc078Kp4H16PSn+oeaU+40WUK/vuex7ya" +
            "miOBnHIdcbwhqU9DvpTLPL10crXXNas5NHrj3+ik9+FoN2WoLScRyiNLdVbcp0ynRW7S0SbPowoI" +
            "uuJ0v0KmzrAQqFnLMukwSXxyGZ7NMwoOHtDk0a1ZLfGBmYN34LaL761GzEMNjATJTK0RDGOP/cq4" +
            "pHwROzUYILwQoYIrNvMA6WI4U28DdoD8PYa6N8EKNIjOk/PEUn2Q5E8DkaWzJvljoFLn5bWAA49L" +
            "uu0WmIEUVJxVvIcwOexQAQ8F+QbQ2ulQ2g1f/8meII+Q3yT73K3piGBNih/Ckmf3jU5ge4ChdGvB" +
            "Eh1a+axy9zTLuD0PW6o3S+qHipTb/k1kxyLjKrxCdP84XNODSSgfrk9hgLRmCN6yQ+SK1Jk7WwCe" +
            "d47IJLDVu6z+5KwGYAqba0nzR+mIMkFb9UQKTaWperBKab0kN6s3bQFaN79VmDf+vRIMwr0KD5gi" +
            "KJtPnHqBFkoeH4OkC+Swq4LXSQATPrp9Z0TxaMWh5JjbaXiPWwjW2DYO/xTUAshDAkHDMPUr058C" +
            "svgACgrTSM1abL5L8cdz71/9ebpTiazk61dm6h3iwKm8eMxosVrg/c31Bqxv7kMHUYRpjRPbxvkv" +
            "R0R3Kt0j0DcxlrJjOL39Akc28WDc3wiwjLIaRcS+QAcy6KflnqKFlRMdnqqEh6NLcmT9gLe2lg9k" +
            "kiega64lIIKzliiRWSsvKePzSrL3sf6i7VZPz1sC7yzbbxhaHE0w8aVi36GDpTGBixk2Q3KOP/tg" +
            "50xU9CwaomnwLDMsFIQnGUqoiESW4WXmqIKbmn1NUpX8MFWnlAmeqgS6m2ET5+lIIo5gwYZh0zKw" +
            "iTk3o7FGmQnR5jqkKjFlNmQ9Z1aVhBq2rFG6LkckpJaiFpbISfE8sLLSsnx5Ols2utENaA89ND3S" +
            "gpOTDwafzy0ekT59PXv2BWLiREVbjbBAOOe58DYWmqxp9jfi0pMZ8Oi7Z9i4Z4lmJORn43CYWuT3" +
            "nyGDaSVin5Rg6B6BjW73g0CQ+vjyz34KkHVzT/f6QuyR0NC0cDDnot5KYIS+Hxij8XaylKR/xphV" +
            "SZTl8EraOmKJVABUPKmHl31JvtNADibouy4+NCMywwzRPmM/RbO2KTZjqpIjLJBbPv7NNm1MbpxD" +
            "lJbcthYO+l1fKgi/777arEIrt70A/2iumGfNJTZZRsiWurvh19mrSGGFvT7COQ1mTsOz9g4AP8p6" +
            "nGLfNiagYmJLBI1LiYoAQKpgVDNz4sdCm+YZzuNKFEKThEGKfnjN0hibx0wHecu7bXm+QcjwOlt+" +
            "584A+2v04/znpU4x5kXqaSOWJqJlCuIoBsrkzaw9iLVQ4THK0yXGJfz01m9VfWgVP/W6JVi8KPfm" +
            "7cf+bNcKfIblHD1BNJSbB6SdLwj3eBkNnZvBwvP73cR58WJ1zi7cAVs4RvhCQd3FhS/P+wsYWHTr" +
            "MWwhESyTHdDjsY6+SzickYHV2vPXLHsmLxaWrKpUiS4LcO5Ro93VeKQprbSYaBK8y+ZO+S0j0Mrc" +
            "Qh9b/c2jxkohkRoH6fVGjdrDT/IggEV0RRpgnppHhbfF5LWqVhujRbSGSd3zJ5AT</xenc:CipherValue>" +
            "    </xenc:CipherData>" +
            "  </xenc:EncryptedData>" +
            "</Security>";

    @Ignore
    @Test
    public void testSamlDecipher() throws Exception {
        Document document = getDecipheredDoc();
        Node subject = document.getElementsByTagNameNS("urn:oasis:names:tc:SAML:2.0:assertion", "Subject").item(0);
        assertNotNull(subject);
        NodeList subjectChildNodes = subject.getChildNodes();
        assertEquals(2, subjectChildNodes.getLength());
        Node nameId = subjectChildNodes.item(0);
        assertNotNull(nameId);
        assertEquals("cvuser1", nameId.getTextContent());
    }

    @Ignore
    @Test
    public void testFixRootNode() throws Exception {
        Document document = getDecipheredDoc();
        String result = ProductionTool.fixRootNode(getStringFromDoc(document));
        String replace = result.replaceAll("\\r\\n", "");
        assertTrue(replace.startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<saml2:Assertion xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\""));
    }

    private static Document getDecipheredDoc() throws Exception {
        Init.init();

        // fill in private Calvalus key here to successfully run the test; see \projects\ongoing\CODE\integration\authentication\keys\calvalus_priv.der
        String keyPath = null;

        if (keyPath == null) {
            fail("Need to set Calvalus private key");
        }

        PrivateKey myPrivKey = readPrivateKey(keyPath);
        Document document = parseXml(encryptedSaml);
        document = getDecipheredDoc(myPrivKey, document);
        return document;
    }

    private static Document getDecipheredDoc(PrivateKey myPrivKey, Document document) throws Exception {
        Element encryptedDataElement = (Element) document.getElementsByTagNameNS(EncryptionConstants.EncryptionSpecNS, EncryptionConstants._TAG_ENCRYPTEDDATA).item(0);
        XMLCipher xmlCipher = XMLCipher.getInstance();
        xmlCipher.init(XMLCipher.DECRYPT_MODE, null);
        xmlCipher.setKEK(myPrivKey);
        return xmlCipher.doFinal(document, encryptedDataElement);
    }

    private static PrivateKey readPrivateKey(String key) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        Path path = Paths.get(key);
        byte[] privKeyByteArray = Files.readAllBytes(path);

        PKCS8EncodedKeySpec privKeySpec = new PKCS8EncodedKeySpec(privKeyByteArray);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(privKeySpec);
    }

    private static Document parseXml(String xml) throws SAXException, IOException, ParserConfigurationException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputStream inputStream = new ByteArrayInputStream(xml.getBytes());
        return db.parse(inputStream);
    }

    private String getStringFromDoc(Document doc)    {
        try {
            DOMSource domSource = new DOMSource(doc);
            StringWriter writer = new StringWriter();
            StreamResult result = new StreamResult(writer);
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.transform(domSource, result);
            writer.flush();
            return writer.toString();
        }
        catch(TransformerException ex) {
            throw new IllegalArgumentException("Unable to parse SAML token", ex);
        }
    }

}