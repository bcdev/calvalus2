package com.bc.calvalus.portal.server;

import org.apache.xml.security.encryption.XMLCipher;
import org.apache.xml.security.utils.EncryptionConstants;
import org.jasig.cas.client.authentication.AttributePrincipalImpl;
import org.jasig.cas.client.validation.AbstractCasProtocolUrlBasedTicketValidator;
import org.jasig.cas.client.validation.Assertion;
import org.jasig.cas.client.validation.AssertionImpl;
import org.jasig.cas.client.validation.TicketValidationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.HashMap;

public class SamlCreateTicketValidator extends AbstractCasProtocolUrlBasedTicketValidator {

    public SamlCreateTicketValidator(String casServerUrlPrefix) {
        super(casServerUrlPrefix);
    }

    @Override
    protected String getUrlSuffix() {
        return "/samlCreate2";
    }

    @Override
    protected Assertion parseResponseFromServer(String response) throws TicketValidationException {

        org.apache.xml.security.Init.init();

        try {
            PrivateKey myPrivKey = readPrivateKey("/home/cvop/.calvalus/calvalus_priv.der");
            Document document = parseXml(response);
            document = decipher(myPrivKey, document);

            Node subjectNode = document.getElementsByTagNameNS("urn:oasis:names:tc:SAML:2.0:assertion", "Subject").item(0);
            NodeList subjectChildNodes = subjectNode.getChildNodes();
            Node nameId = subjectChildNodes.item(0);
            final String subject = nameId.getTextContent();

            HashMap<String, Object> attributes = new HashMap<>();
            attributes.put("rawSamlToken", document);

            return new AssertionImpl(new AttributePrincipalImpl(subject, attributes));
        } catch (Exception ex) {
            System.out.println("Evaluation of SAML token failed. Response was: " + response);
            throw new TicketValidationException("Evaluation of SAML token failed.", ex);
        }
    }

    private static Document decipher(PrivateKey myPrivKey, Document document) throws Exception {
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


//
//    private static Object extractAttributeValue(Attribute attr) {
//
//        final List<String> values = new ArrayList<>();
//
//        for (XMLObject value : attr.getAttributeValues()) {
//            String v = null;
//            if (value instanceof AttributeValue) {
//                v = ((AttributeValue) value).getOrderedChildren().toString();
//            } else if (value instanceof XSStringImpl) {
//                v = ((XSStringImpl) value).getValue();
//            }
//
//            if (v != null) {
//                values.add(v);
//            } else {
//                logger.info("Ignoring attribute value for " + attr.getName() + "=" + value);
//            }
//        }
//
//        switch (values.size()) {
//            case 0:
//                return null;
//            case 1:
//                return values.get(0);
//            default:
//                return values;
//        }
//    }

}
