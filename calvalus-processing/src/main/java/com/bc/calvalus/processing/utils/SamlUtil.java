package com.bc.calvalus.processing.utils;

import org.joda.time.DateTime;
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
import org.opensaml.saml2.core.Response;
import org.opensaml.saml2.core.Subject;
import org.opensaml.saml2.core.impl.ResponseMarshaller;
import org.opensaml.xml.ConfigurationException;
import org.opensaml.xml.XMLObjectBuilderFactory;
import org.opensaml.xml.io.MarshallingException;
import org.opensaml.xml.schema.XSString;
import org.opensaml.xml.security.SecurityConfiguration;
import org.opensaml.xml.security.SecurityException;
import org.opensaml.xml.security.SecurityHelper;
import org.opensaml.xml.security.credential.Credential;
import org.opensaml.xml.security.x509.BasicX509Credential;
import org.opensaml.xml.signature.Signature;
import org.opensaml.xml.signature.SignatureException;
import org.opensaml.xml.signature.Signer;
import org.opensaml.xml.signature.impl.SignatureBuilder;
import org.opensaml.xml.util.XMLHelper;
import org.w3c.dom.Element;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.namespace.QName;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * <?xml version="1.0" encoding="UTF-8"?>
 * <saml2:Assertion Version="2.0" xmlns:saml2="urn:oasis:names:tc:SAML:2.0:assertion">
 *     <saml2:Issuer>cas</saml2:Issuer>
 *     <saml2:Subject>
 *         <saml2:NameID>cd_calvalus</saml2:NameID>
 *     </saml2:Subject>
 *     <saml2:Conditions NotBefore="2017-06-23T10:27:05.354Z" NotOnOrAfter="2017-06-24T10:27:05.354Z"/>
 *     <saml2:AuthnStatement>
 *         <saml2:AuthnContext>
 *             <saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef>
 *         </saml2:AuthnContext>
 *     </saml2:AuthnStatement>
 *     <saml2:AttributeStatement>
 *         <saml2:Attribute Name="groups">
 *             <saml2:AttributeValue
 *                 xmlns:xs="http://www.w3.org/2001/XMLSchema"
 *                 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">calvalus,testproject</saml2:AttributeValue>
 *         </saml2:Attribute>
 *         <saml2:Attribute Name="email">
 *             <saml2:AttributeValue
 *                 xmlns:xs="http://www.w3.org/2001/XMLSchema"
 *                 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="xs:string">calvalustest@code.de</saml2:AttributeValue>
 *         </saml2:Attribute>
 *     </saml2:AttributeStatement>
 * </saml2:Assertion>
 */
public class SamlUtil
{
    XMLObjectBuilderFactory factory;
    MessageDigest SHA256;

    SamlUtil() throws ConfigurationException, NoSuchAlgorithmException {
        DefaultBootstrap.bootstrap();
        factory = Configuration.getBuilderFactory();
        SHA256 = MessageDigest.getInstance("SHA-256");
    }
    private SAMLObject build(QName name) {
        return ((SAMLObjectBuilder) factory.getBuilder(name)).buildObject();
    }

    private Signature buildSignature(QName name) {
        return ((SignatureBuilder) factory.getBuilder(name)).buildObject();
    }

    private XSString buildString(QName name) {
        return (XSString) factory.getBuilder(XSString.TYPE_NAME).buildObject(name, XSString.TYPE_NAME);
    }
 
	public static void main(String[] args) {
    	try {
			String issuer = "cas";
			String subject = "cd_calvalus";
			Map<String,String> attributes = new HashMap<>();
			attributes.put("groups", "calvalus,testproject");
			attributes.put("email", "calvalustest@code.de");
			int timeoutSeconds = 60*60*24;

            SamlUtil util = new SamlUtil();
            Response response = util.build(issuer, subject, attributes, new DateTime(), timeoutSeconds);
			String samlAssertion = util.pp(response);
			System.out.println("Assertion: " + samlAssertion);

            String certificateAliasName = "cas_certificate";
            String password = "secret";
            String keyStoreFileName = "/home/boe/tmp/code/caskeystore3.keys";
            Credential credentials = readCredentials(password, keyStoreFileName, certificateAliasName);

            Response response2 = util.sign(response, credentials);
            String samlResponse = util.pp(response2);
            System.out.println("Signed response:" + samlResponse);

		} catch (Exception e) {
			e.printStackTrace();
		}
 
	}

    public static Credential readCredentials(String passwordValue, String keyStoreFileName, String certificateAliasName) throws CertificateException, NoSuchAlgorithmException, IOException, KeyStoreException, UnrecoverableEntryException {
        char[] password = passwordValue.toCharArray();
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        try (FileInputStream fis = new FileInputStream(keyStoreFileName)) {
            ks.load(fis, password);
        }
        KeyStore.PrivateKeyEntry pkEntry = (KeyStore.PrivateKeyEntry) ks.getEntry(certificateAliasName, new KeyStore.PasswordProtection(password));
        PrivateKey pk = pkEntry.getPrivateKey();
        X509Certificate certificate = (X509Certificate) pkEntry.getCertificate();
        BasicX509Credential credential = new BasicX509Credential();
        credential.setEntityCertificate(certificate);
        credential.setPrivateKey(pk);
        System.out.println("Private Key" + pk.toString());
        return credential;
    }

    public String pp(Response response) throws MarshallingException {
        Element element = new ResponseMarshaller().marshall(response);
        return XMLHelper.prettyPrintXML(element);
    }

    public String toString(Response response) throws MarshallingException {
        Element element = new ResponseMarshaller().marshall(response);
        return XMLHelper.nodeToString(element);
    }

    public Response build(String issuerValue, String subjectValue, Map<String, String> attributesValue, DateTime timestamp, int timeoutValue) throws ConfigurationException {

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

        Response response = (Response) build(Response.DEFAULT_ELEMENT_NAME);
        response.getAssertions().add(assertion);
        return response;
    }

    public Response sign(Response response, Credential credentials) throws SecurityException, MarshallingException, SignatureException {
        Signature signature = (Signature) buildSignature(Signature.DEFAULT_ELEMENT_NAME);
        signature.setSigningCredential(credentials);
        SecurityConfiguration secConfig = Configuration.getGlobalSecurityConfiguration();
        //String keyInfoGeneratorProfile = "XMLSignature";
        SecurityHelper.prepareSignatureParams(signature, credentials, secConfig, null /*keyInfoGeneratorProfile*/);
        Assertion assertion = response.getAssertions().get(0);
        assertion.setSignature(signature);
        Configuration.getMarshallerFactory().getMarshaller(response).marshall(response);
        Signer.signObject(signature);
        return response;
    }

    public byte[] sha256(String request) throws UnsupportedEncodingException {
        return SHA256.digest(request.getBytes("UTF-8"));
    }


    public String encryptRsaAes(String message, PublicKey rsaKey) throws Exception {
        KeyGenerator keygen = KeyGenerator.getInstance("AES");
        keygen.init(128);
        Key key = keygen.generateKey();

        Cipher rsa = Cipher.getInstance("RSA/ECB/PKCS1Padding");
        rsa.init(Cipher.ENCRYPT_MODE, rsaKey);
        byte[] encryptedAesKey = rsa.doFinal(key.getEncoded());
        Base64.Encoder base64 = Base64.getEncoder();
        String encodedAesKey = base64.encodeToString(encryptedAesKey);

        Cipher aes = Cipher.getInstance("AES");
        aes.init(Cipher.ENCRYPT_MODE, key);
        byte[] encryptedHashAndSaml = aes.doFinal(message.getBytes("UTF-8"));
        String encodedHashAndSaml = base64.encodeToString(encryptedHashAndSaml);

        return encodedAesKey + ' ' + encodedHashAndSaml;
    }

    public String decryptCalvalusToken(String calvalusToken, PrivateKey rsaKey) throws Exception {
        Base64.Decoder base64 = Base64.getDecoder();

        int p1 = calvalusToken.indexOf(" ");
        String aesKeyPart = calvalusToken.substring(0, p1);
        String hashAndSamlPart = calvalusToken.substring(p1 + 1);

        System.out.println(base64.decode(aesKeyPart).length);
        System.out.println(aesKeyPart);

        Cipher rsa = Cipher.getInstance("RSA/ECB/PKCS1Padding");
        rsa.init(Cipher.DECRYPT_MODE, rsaKey);
        byte[] aesKeyCode = rsa.doFinal(base64.decode(aesKeyPart.getBytes()));
        SecretKey aesKey = new SecretKeySpec(aesKeyCode, "AES");

        Cipher aes = Cipher.getInstance("AES");
        aes.init(Cipher.DECRYPT_MODE, aesKey);
        return new String(aes.doFinal(base64.decode(hashAndSamlPart.getBytes())), "UTF-8");
    }
}
