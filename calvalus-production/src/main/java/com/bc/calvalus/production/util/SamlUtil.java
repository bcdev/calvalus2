package com.bc.calvalus.production.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.opensaml.Configuration;
import org.opensaml.DefaultBootstrap;
import org.opensaml.common.SAMLObject;
import org.opensaml.common.SAMLObjectBuilder;
import org.opensaml.common.SAMLVersion;
import org.opensaml.saml2.core.Assertion;
import org.opensaml.saml2.core.Attribute;
import org.opensaml.saml2.core.AttributeStatement;
import org.opensaml.saml2.core.AttributeValue;
import org.opensaml.saml2.core.AuthnContext;
import org.opensaml.saml2.core.AuthnContextClassRef;
import org.opensaml.saml2.core.AuthnStatement;
import org.opensaml.saml2.core.Conditions;
import org.opensaml.saml2.core.Issuer;
import org.opensaml.saml2.core.NameID;
import org.opensaml.saml2.core.Subject;
import org.opensaml.saml2.core.impl.AssertionMarshaller;
import org.opensaml.security.SAMLSignatureProfileValidator;
import org.opensaml.xml.ConfigurationException;
import org.opensaml.xml.XMLObjectBuilderFactory;
import org.opensaml.xml.io.MarshallingException;
import org.opensaml.xml.io.UnmarshallingException;
import org.opensaml.xml.parse.BasicParserPool;
import org.opensaml.xml.parse.XMLParserException;
import org.opensaml.xml.schema.XSString;
import org.opensaml.xml.security.SecurityConfiguration;
import org.opensaml.xml.security.SecurityException;
import org.opensaml.xml.security.SecurityHelper;
import org.opensaml.xml.security.credential.Credential;
import org.opensaml.xml.security.x509.BasicX509Credential;
import org.opensaml.xml.signature.Signature;
import org.opensaml.xml.signature.SignatureException;
import org.opensaml.xml.signature.SignatureValidator;
import org.opensaml.xml.signature.Signer;
import org.opensaml.xml.signature.impl.SignatureBuilder;
import org.opensaml.xml.util.XMLHelper;
import org.opensaml.xml.validation.ValidationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.namespace.QName;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Map;
import java.util.Random;

/**
 * <?xml version="1.0" encoding="UTF-8"?>
 * <saml2:Assertion Version="2.0" xmlns:saml2="urn:oasis:names:tc:SAML:2.0:assertion">
 *     <saml2:Issuer>cas</saml2:Issuer>
 *     <saml2:Subject>
 *         <saml2:NameID>user uid</saml2:NameID>
 *     </saml2:Subject>
 *     <saml2:Conditions NotBefore="2017-06-23T10:27:05.354Z" NotOnOrAfter="2017-06-24T10:27:05.354Z"/>
 *     <saml2:AuthnStatement>
 *         <saml2:AuthnContext>
 *             <saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef>
 *         </saml2:AuthnContext>
 *     </saml2:AuthnStatement>
 *     <saml2:AttributeStatement>
 *         <saml2:Attribute Name="uid">
 *             <saml2:AttributeValue xsi:type="xsd:string">cd_calvalus</saml2:AttributeValue>
 *         </saml2:Attribute>
 *         <saml2:Attribute Name="groups">
 *             <saml2:AttributeValue xsi:type="xs:string">calvalus,testproject</saml2:AttributeValue>
 *         </saml2:Attribute>
 *         <saml2:Attribute Name="email">
 *             <saml2:AttributeValue xsi:type="xs:string">calvalustest@code.de</saml2:AttributeValue>
 *         </saml2:Attribute>
 *     </saml2:AttributeStatement>
 * </saml2:Assertion>
 */
public class SamlUtil {

    private XMLObjectBuilderFactory xmlObjectBuilderFactory;
    private KeyFactory rsaKeyFactory;
    private CertificateFactory certFactory;
    private MessageDigest SHA256;
    private PublicKey calvalusPublicKey = null;

    public SamlUtil() {
        try {
            DefaultBootstrap.bootstrap();
            xmlObjectBuilderFactory = Configuration.getBuilderFactory();
            rsaKeyFactory = KeyFactory.getInstance("RSA");
            certFactory = CertificateFactory.getInstance("X.509");
            SHA256 = MessageDigest.getInstance("SHA-256");
        } catch (ConfigurationException | NoSuchAlgorithmException | CertificateException e) {
            throw new RuntimeException("exception while initialising Java crypt functions", e);
        }
    }

    public SamlUtil(String publicKey) {
        this();
        try {
            calvalusPublicKey = readPublicKey(publicKey);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
            throw new RuntimeException("exception reading keys and certificates", e);
        }
    }

    public PublicKey getCalvalusPublicKey() {
        return calvalusPublicKey;
    }

    public PublicKey readPublicKey(String fileName) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        File keyFile = new File(fileName);
        byte[] keySequence = new byte[(int) keyFile.length()];
        try (FileInputStream in = new FileInputStream(keyFile)) {
            in.read(keySequence);
        }
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(keySequence);
        PublicKey rsaKey = rsaKeyFactory.generatePublic(publicKeySpec);
        return rsaKey;
    }



    private SAMLObject build(QName name) {
        return ((SAMLObjectBuilder) xmlObjectBuilderFactory.getBuilder(name)).buildObject();
    }

    private Signature buildSignature(QName name) {
        return ((SignatureBuilder) xmlObjectBuilderFactory.getBuilder(name)).buildObject();
    }

    private XSString buildString(QName name) {
        return (XSString) xmlObjectBuilderFactory.getBuilder(XSString.TYPE_NAME).buildObject(name, XSString.TYPE_NAME);
    }
 
    public Credential readCredentials(String privFileName, String certFileName) throws NoSuchAlgorithmException, IOException, InvalidKeySpecException, CertificateException {
        byte[] privateKeyBytes = new byte[(int) new File(privFileName).length()];
        try (FileInputStream in = new FileInputStream(privFileName)) {
            in.read(privateKeyBytes);
        }
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privateKeyBytes);
        PrivateKey privateKey = rsaKeyFactory.generatePrivate(keySpec);

        X509Certificate certificate;
        try (FileInputStream in = new FileInputStream(certFileName)) {
            certificate = (X509Certificate) certFactory.generateCertificate(in);
        }
        BasicX509Credential credential = new BasicX509Credential();
        credential.setEntityCertificate(certificate);
        credential.setPrivateKey(privateKey);
        return credential;

    }

    public String pp(Assertion assertion) throws MarshallingException {
        Element element = new AssertionMarshaller().marshall(assertion);
        return XMLHelper.prettyPrintXML(element);
    }

    public String toString(Assertion assertion) throws MarshallingException {
        Element element = new AssertionMarshaller().marshall(assertion);
        return XMLHelper.nodeToString(element);
    }

    /**
     * Builds SAML response
     * @param issuerValue
     * @param subjectValue
     * @param attributesValue
     * @param timestamp
     * @param timeoutValue
     * @return SAML response with unsigned assertion
     * @throws ConfigurationException
     */
    public Assertion build(String issuerValue, String subjectValue, Map<String, String> attributesValue, DateTime timestamp, int timeoutValue) throws ConfigurationException {

        Assertion assertion = (Assertion) build(Assertion.DEFAULT_ELEMENT_NAME);
        assertion.setVersion(SAMLVersion.VERSION_20);

        Issuer issuer = (Issuer) build(Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(issuerValue);
        assertion.setIssuer(issuer);

        Conditions conditions = (Conditions) build(Conditions.DEFAULT_ELEMENT_NAME);
        conditions.setNotBefore(timestamp);
        conditions.setNotOnOrAfter(timestamp.plusSeconds(timeoutValue));
        assertion.setConditions(conditions);

        Subject subject = (Subject) build(Subject.DEFAULT_ELEMENT_NAME);
        NameID nameId = (NameID) build(NameID.DEFAULT_ELEMENT_NAME);
        nameId.setValue(subjectValue);
        subject.setNameID(nameId);
        assertion.setSubject(subject);

        AuthnStatement authnStatement = (AuthnStatement) build(AuthnStatement.DEFAULT_ELEMENT_NAME);
        AuthnContext authnContext = (AuthnContext) build(AuthnContext.DEFAULT_ELEMENT_NAME);
        AuthnContextClassRef authnContextClassRef = (AuthnContextClassRef) build(AuthnContextClassRef.DEFAULT_ELEMENT_NAME);
        authnContextClassRef.setAuthnContextClassRef("urn:oasis:names:tc:SAML:2.0:ac:classes:Password");
        authnContext.setAuthnContextClassRef(authnContextClassRef);
        authnStatement.setAuthnContext(authnContext);
        assertion.getAuthnStatements().add(authnStatement);

        AttributeStatement attrStatement = (AttributeStatement) build(AttributeStatement.DEFAULT_ELEMENT_NAME);
        for (Map.Entry<String,String> entry : attributesValue.entrySet()) {
            Attribute attribute = (Attribute) build(Attribute.DEFAULT_ELEMENT_NAME);
            attribute.setName(entry.getKey());
            XSString value = buildString(AttributeValue.DEFAULT_ELEMENT_NAME);
            value.setValue(entry.getValue());
            attribute.getAttributeValues().add(value);
            attrStatement.getAttributes().add(attribute);
        }
        assertion.getAttributeStatements().add(attrStatement);

        //Response response = (Response) build(Response.DEFAULT_ELEMENT_NAME);
        //response.getAssertions().add(assertion);
        return assertion;
    }

    /**
     * Signs SAML assertion
     * @param assertion
     * @param credentials
     * @return SAML response with signed assertion
     * @throws SecurityException
     * @throws MarshallingException
     * @throws SignatureException
     */
    public Assertion sign(Assertion assertion, Credential credentials) throws SecurityException, MarshallingException, SignatureException {
        Signature signature = (Signature) buildSignature(Signature.DEFAULT_ELEMENT_NAME);
        signature.setSigningCredential(credentials);
        SecurityConfiguration secConfig = Configuration.getGlobalSecurityConfiguration();
        SecurityHelper.prepareSignatureParams(signature, credentials, secConfig, null);
        //Assertion assertion = response.getAssertions().get(0);
        assertion.setSignature(signature);
        Configuration.getMarshallerFactory().getMarshaller(assertion).marshall(assertion);
        Signer.signObject(signature);
        return assertion;
    }

    public boolean validate(String xmlString, Credential credential) {
        try {
            Document doc;
            try (InputStream inputStream = new ByteArrayInputStream(xmlString.getBytes())) {
                doc = new BasicParserPool().parse(inputStream);
            }
            Element element = doc.getDocumentElement();
            Assertion assertion = (Assertion) Configuration.getUnmarshallerFactory().getUnmarshaller(element).unmarshall(element);
            //Assertion assertion = response.getAssertions().get(0);
            assertion.validate(true);
            if (false) {
                long now = System.currentTimeMillis();
                int maxClockSkew = 1000;
                Conditions conditions = assertion.getConditions();
                if ((conditions == null) || conditions.getNotBefore().isAfter(now + maxClockSkew)
                        || conditions.getNotOnOrAfter().isBefore(now - maxClockSkew)) {
                    String msg = "SAML Assertion failed validity time check "
                            + ((conditions == null) ? "(Condition is missing)" : conditions.getNotBefore().isAfter(now)
                            ? " - token is for the future (Condition " + conditions.getNotBefore() + " is after " + new DateTime(now, DateTimeZone.UTC) + ")"
                            : " - token is expired (Condition " + conditions.getNotOnOrAfter() + " is before " + new DateTime(now, DateTimeZone.UTC) + ")");

                    throw new ValidationException(msg);
                }
            }
            //
            Signature signature = assertion.getSignature();
            SAMLSignatureProfileValidator pv = new SAMLSignatureProfileValidator();
            pv.validate(signature);
            // determine signing entity and use its trusted public certificate
/*
            String issuer = null;
            X509Certificate certificate;
            try {
                issuer = samlAssertion.getIssuer();
                certificate = KeyUtil.getTrustedCertificate(issuer);
            } catch (Exception e) {
                String msg = "Failed to retreive trusted public key for " + issuer;
                throw new ValidationException(msg);
            }
            // check signature validity
            BasicX509Credential credential = new BasicX509Credential();
            credential.setEntityCertificate(certificate);
*/
            SignatureValidator sigValidator = new SignatureValidator(credential);
            sigValidator.validate(signature);
            return true;
        } catch (ValidationException|IOException|XMLParserException|UnmarshallingException e) {
            e.printStackTrace();
            return false;
        }
    }


    /**
     * Hashes request string
     * @param request
     * @return SHA256 digest of request string
     * @throws UnsupportedEncodingException
     */
    public byte[] sha256(String request) throws UnsupportedEncodingException {
        return SHA256.digest(request.getBytes("UTF-8"));
    }

    /**
     * Encrypts hash+SAML with generated AES key and AES key with provided RSA key
     * @param message
     * @param rsaKey
     * @return triple of base64-encoded IV, encrypted AES key, encrypted hash+SAML, separated by space
     * @throws Exception
     */
    public String encryptRsaAes(String message, PublicKey rsaKey) throws NoSuchAlgorithmException, IOException, NoSuchPaddingException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException, InvalidAlgorithmParameterException {
        Base64.Encoder base64 = Base64.getEncoder();

        KeyGenerator keygen = KeyGenerator.getInstance("AES");
        keygen.init(128);
        Key key = keygen.generateKey();

        byte[] iv = new byte[16];
        new Random().nextBytes(iv);
        String encodedIV = base64.encodeToString(iv);

        Cipher rsa = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
        rsa.init(Cipher.ENCRYPT_MODE, rsaKey);
        byte[] encryptedAesKey = rsa.doFinal(key.getEncoded());
        String encodedAesKey = base64.encodeToString(encryptedAesKey);

        Cipher aes = Cipher.getInstance("AES/CBC/PKCS5Padding");
        aes.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
        byte[] encryptedHashAndSaml = aes.doFinal(message.getBytes("UTF-8"));
        String encodedHashAndSaml = base64.encodeToString(encryptedHashAndSaml);

        return encodedIV + ' ' + encodedAesKey + ' ' + encodedHashAndSaml;
    }

    /**
     * Decrypts encrypted hash+SAML using provided RSA key and encrypted AES key
     * @param calvalusToken triple of base64-encoded IV, encrypted AES key, encrypted hash+SAML, separated by space
     * @param rsaKey
     * @return pair of base64-encoded hash and SAML
     * @throws Exception
     */
    public String decryptCalvalusToken(String calvalusToken, PrivateKey rsaKey) throws Exception {
        Base64.Decoder base64 = Base64.getDecoder();

        int p1 = calvalusToken.indexOf(" ");
        int p2 = calvalusToken.indexOf(" ", p1 + 1);
        String ivPart = calvalusToken.substring(0, p1);
        String aesKeyPart = calvalusToken.substring(p1 + 1, p2);
        String hashAndSamlPart = calvalusToken.substring(p2 + 1);

        Cipher rsa = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
        rsa.init(Cipher.DECRYPT_MODE, rsaKey);
        byte[] aesKeyCode = rsa.doFinal(base64.decode(aesKeyPart.getBytes()));
        SecretKey aesKey = new SecretKeySpec(aesKeyCode, "AES");

        Cipher aes = Cipher.getInstance("AES/CBC/PKCS5Padding");
        aes.init(Cipher.DECRYPT_MODE, aesKey, new IvParameterSpec(base64.decode(ivPart)));
        return new String(aes.doFinal(base64.decode(hashAndSamlPart.getBytes())), "UTF-8");
    }
}
