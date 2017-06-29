package com.bc.calvalus.processing.utils;

import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;
import org.opensaml.saml2.core.Response;
import org.opensaml.xml.ConfigurationException;
import org.opensaml.xml.security.credential.Credential;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SamlUtilTest
{
    static String SIMPLE_SAML_RESPONSE =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<saml2p:Response Version=\"2.0\" xmlns:saml2p=\"urn:oasis:names:tc:SAML:2.0:protocol\">\n" +
        "    <saml2:Assertion Version=\"2.0\" xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">\n" +
        "        <saml2:Issuer>cas</saml2:Issuer>\n" +
        "        <saml2:Subject>\n" +
        "            <saml2:NameID>cd_calvalus</saml2:NameID>\n" +
        "        </saml2:Subject>\n" +
        "        <saml2:Conditions NotBefore=\"2017-06-23T10:27:05.354Z\" NotOnOrAfter=\"2017-06-24T10:27:05.354Z\"/>\n" +
        "        <saml2:AuthnStatement>\n" +
        "            <saml2:AuthnContext>\n" +
        "                <saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef>\n" +
        "            </saml2:AuthnContext>\n" +
        "        </saml2:AuthnStatement>\n" +
        "        <saml2:AttributeStatement>\n" +
        "            <saml2:Attribute Name=\"groups\">\n" +
        "                <saml2:AttributeValue\n" +
        "                    xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"\n" +
        "                    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalus,testproject</saml2:AttributeValue>\n" +
        "            </saml2:Attribute>\n" +
        "            <saml2:Attribute Name=\"email\">\n" +
        "                <saml2:AttributeValue\n" +
        "                    xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"\n" +
        "                    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalustest@code.de</saml2:AttributeValue>\n" +
        "            </saml2:Attribute>\n" +
        "        </saml2:AttributeStatement>\n" +
        "    </saml2:Assertion>\n" +
        "</saml2p:Response>\n";

    static String SIGNED_SAML_RESPONSE =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<saml2p:Response Version=\"2.0\" xmlns:saml2p=\"urn:oasis:names:tc:SAML:2.0:protocol\">\n" +
        "    <saml2:Assertion Version=\"2.0\"\n" +
        "        xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">\n" +
        "        <saml2:Issuer>cas</saml2:Issuer>\n" +
        "        <ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">\n" +
        "            <ds:SignedInfo>\n" +
        "                <ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/>\n" +
        "                <ds:SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#dsa-sha1\"/>\n" +
        "                <ds:Reference URI=\"\">\n" +
        "                    <ds:Transforms>\n" +
        "                        <ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/>\n" +
        "                        <ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\">\n" +
        "                            <ec:InclusiveNamespaces PrefixList=\"xs\" xmlns:ec=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/>\n" +
        "                        </ds:Transform>\n" +
        "                    </ds:Transforms>\n" +
        "                    <ds:DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"/>\n" +
        "                    <ds:DigestValue>jsFpcQ9z8tMHdtk0/oYwP8kAxBc=</ds:DigestValue>\n" +
        "                </ds:Reference>\n" +
        "            </ds:SignedInfo>\n" +
        "            IEYJZHtDALBgcqhkjOOAQDBQAwbDEQMA4GA1UEBhMHVW5rbm93bjEQMA4G\n" +
        "A1UECBMHVW5rbm93</ds:SignatureValue>\n" +
        "            <ds:KeyInfo>\n" +
        "                <ds:X509Data>\n" +
        "                    <ds:X509Certificate>MIIDNTCCAvOgAwIBAgIEYJZHtDALBgcqhkjOOAQDBQAwbDEQMA4GA1UEBhMHVW5rbm93bjEQMA4G\n" +
        "A1UECBMHVW5rbm93bjEQMA4GA1UEBxMHVW5rbm93bjEQMA4GA1UEChMHY29kZS5kZTEQMA4GA1UE\n" +
        "CxMHVW5rbm93bjEQMA4GA1UEAxMHVW5rbm93bjAeFw0xNzA2MjMxNDE5MjBaFw0xNzA5MjExNDE5\n" +
        "MjBaMGwxEDAOBgNVBAYTB1Vua25vd24xEDAOBgNVBAgTB1Vua25vd24xEDAOBgNVBAcTB1Vua25v\n" +
        "d24xEDAOBgNVBAoTB2NvZGUuZGUxEDAOBgNVBAsTB1Vua25vd24xEDAOBgNVBAMTB1Vua25vd24w\n" +
        "ggG4MIIBLAYHKoZIzjgEATCCAR8CgYEA/X9TgR11EilS30qcLuzk5/YRt1I870QAwx4/gLZRJmlF\n" +
        "XUAiUftZPY1Y+r/F9bow9subVWzXgTuAHTRv8mZgt2uZUKWkn5/oBHsQIsJPu6nX/rfGG/g7V+fG\n" +
        "qKYVDwT7g/bTxR7DAjVUE1oWkTL2dfOuK2HXKu/yIgMZndFIAccCFQCXYFCPFSMLzLKSuYKi64QL\n" +
        "8Fgc9QKBgQD34aCF1ps93su8q1w2uFe5eZSvu/o66oL5V0wLPQeCZ1FZV4661FlP5nEHEIGAtEkW\n" +
        "cSPoTCgWE7fPCTKMyKbhPBZ6i1R8jSjgo64eK7OmdZFuo38L+iE1YvH7YnoBJDvMpPG+qFGQiaiD\n" +
        "3+Fa5Z8GkotmXoB7VSVkAUw7/s9JKgOBhQACgYEAz41IKVUQ13VqX2IqPUoTpmZ0ZBD+XJHQkuZh\n" +
        "Pxw6ZNkPlZbLdbAlC7vSHlC5d3FsOMbD7i1mFQ7KmaDYBb0rsivEw4uupboCu8Q8iBvxl1AjFtRp\n" +
        "+io5A3jjPBbhtt8C3ZFSdj0b/iFJwp9ub51pDjbkUbBXmrX3Sm4Momy/BzmjITAfMB0GA1UdDgQW\n" +
        "BBS+HuorU5c1yljoTigZ6SGeNP86oDALBgcqhkjOOAQDBQADLwAwLAIUHkZUNqCb6VjhxbW9vcRh\n" +
        "P8VfV8cCFClquisEfDHmF2u6Kl3OMWs4HLD9</ds:X509Certificate>\n" +
        "                </ds:X509Data>\n" +
        "            </ds:KeyInfo>\n" +
        "        </ds:Signature>\n" +
        "        <saml2:Subject>\n" +
        "            <saml2:NameID>cd_calvalus</saml2:NameID>\n" +
        "        </saml2:Subject>\n" +
        "        <saml2:Conditions NotBefore=\"2017-06-23T10:27:05.354Z\" NotOnOrAfter=\"2017-06-24T10:27:05.354Z\"/>\n" +
        "        <saml2:AuthnStatement>\n" +
        "            <saml2:AuthnContext>\n" +
        "                <saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef>\n" +
        "            </saml2:AuthnContext>\n" +
        "        </saml2:AuthnStatement>\n" +
        "        <saml2:AttributeStatement>\n" +
        "            <saml2:Attribute Name=\"groups\">\n" +
        "                <saml2:AttributeValue\n" +
        "                    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalus,testproject</saml2:AttributeValue>\n" +
        "            </saml2:Attribute>\n" +
        "            <saml2:Attribute Name=\"email\">\n" +
        "                <saml2:AttributeValue\n" +
        "                    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalustest@code.de</saml2:AttributeValue>\n" +
        "            </saml2:Attribute>\n" +
        "        </saml2:AttributeStatement>\n" +
        "    </saml2:Assertion>\n" +
        "</saml2p:Response>\n";

    public static final String HASH_AND_SAML_RESPONSE = "8eWW6PfyhxTVhEfPkRiWZ3BHBjN+q0t043IoIrBYyuM= <?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<saml2p:Response Version=\"2.0\" xmlns:saml2p=\"urn:oasis:names:tc:SAML:2.0:protocol\"><saml2:Assertion Version=\"2.0\" xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"><saml2:Issuer>cas</saml2:Issuer><ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\"><ds:SignedInfo><ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/><ds:SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#dsa-sha1\"/><ds:Reference URI=\"\"><ds:Transforms><ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/><ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"><ec:InclusiveNamespaces PrefixList=\"xs\" xmlns:ec=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/></ds:Transform></ds:Transforms><ds:DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"/><ds:DigestValue>jsFpcQ9z8tMHdtk0/oYwP8kAxBc=</ds:DigestValue></ds:Reference></ds:SignedInfo><ds:SignatureValue>TO3J92WEr5P1fWJpx538BPkPqECLuFo/mcYCuxIzHK5mnEqHEC44Uw==</ds:SignatureValue><ds:KeyInfo><ds:X509Data><ds:X509Certificate>MIIDNTCCAvOgAwIBAgIEYJZHtDALBgcqhkjOOAQDBQAwbDEQMA4GA1UEBhMHVW5rbm93bjEQMA4G\n" +
            "A1UECBMHVW5rbm93bjEQMA4GA1UEBxMHVW5rbm93bjEQMA4GA1UEChMHY29kZS5kZTEQMA4GA1UE\n" +
            "CxMHVW5rbm93bjEQMA4GA1UEAxMHVW5rbm93bjAeFw0xNzA2MjMxNDE5MjBaFw0xNzA5MjExNDE5\n" +
            "MjBaMGwxEDAOBgNVBAYTB1Vua25vd24xEDAOBgNVBAgTB1Vua25vd24xEDAOBgNVBAcTB1Vua25v\n" +
            "d24xEDAOBgNVBAoTB2NvZGUuZGUxEDAOBgNVBAsTB1Vua25vd24xEDAOBgNVBAMTB1Vua25vd24w\n" +
            "ggG4MIIBLAYHKoZIzjgEATCCAR8CgYEA/X9TgR11EilS30qcLuzk5/YRt1I870QAwx4/gLZRJmlF\n" +
            "XUAiUftZPY1Y+r/F9bow9subVWzXgTuAHTRv8mZgt2uZUKWkn5/oBHsQIsJPu6nX/rfGG/g7V+fG\n" +
            "qKYVDwT7g/bTxR7DAjVUE1oWkTL2dfOuK2HXKu/yIgMZndFIAccCFQCXYFCPFSMLzLKSuYKi64QL\n" +
            "8Fgc9QKBgQD34aCF1ps93su8q1w2uFe5eZSvu/o66oL5V0wLPQeCZ1FZV4661FlP5nEHEIGAtEkW\n" +
            "cSPoTCgWE7fPCTKMyKbhPBZ6i1R8jSjgo64eK7OmdZFuo38L+iE1YvH7YnoBJDvMpPG+qFGQiaiD\n" +
            "3+Fa5Z8GkotmXoB7VSVkAUw7/s9JKgOBhQACgYEAz41IKVUQ13VqX2IqPUoTpmZ0ZBD+XJHQkuZh\n" +
            "Pxw6ZNkPlZbLdbAlC7vSHlC5d3FsOMbD7i1mFQ7KmaDYBb0rsivEw4uupboCu8Q8iBvxl1AjFtRp\n" +
            "+io5A3jjPBbhtt8C3ZFSdj0b/iFJwp9ub51pDjbkUbBXmrX3Sm4Momy/BzmjITAfMB0GA1UdDgQW\n" +
            "BBS+HuorU5c1yljoTigZ6SGeNP86oDALBgcqhkjOOAQDBQADLwAwLAIUHkZUNqCb6VjhxbW9vcRh\n" +
            "P8VfV8cCFClquisEfDHmF2u6Kl3OMWs4HLD9</ds:X509Certificate></ds:X509Data></ds:KeyInfo></ds:Signature><saml2:Subject><saml2:NameID>cd_calvalus</saml2:NameID></saml2:Subject><saml2:Conditions NotBefore=\"2017-06-23T10:27:05.354Z\" NotOnOrAfter=\"2017-06-24T10:27:05.354Z\"/><saml2:AuthnStatement><saml2:AuthnContext><saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef></saml2:AuthnContext></saml2:AuthnStatement><saml2:AttributeStatement><saml2:Attribute Name=\"groups\"><saml2:AttributeValue xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalus,testproject</saml2:AttributeValue></saml2:Attribute><saml2:Attribute Name=\"email\"><saml2:AttributeValue xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalustest@code.de</saml2:AttributeValue></saml2:Attribute></saml2:AttributeStatement></saml2:Assertion></saml2p:Response>";

    SamlUtil util;
    public SamlUtilTest() throws NoSuchAlgorithmException, ConfigurationException {
        util = new SamlUtil();
    }

    @Test
    public void testSamlToken() throws Exception {
        Response response = createSamlToken();
        String formattedAssertion = util.pp(response);
        assertEquals("SAML token differs", SIMPLE_SAML_RESPONSE, formattedAssertion);
    }

    public Response createSamlToken() throws Exception {
        String issuer = "cas";
        String subject = "cd_calvalus";
        Map<String,String> attributes = new HashMap<>();
        attributes.put("groups", "calvalus,testproject");
        attributes.put("email", "calvalustest@code.de");
        int timeoutSeconds = 60*60*24;

        Response response = util.build(issuer, subject, attributes, new DateTime("2017-06-23T10:27:05.354Z"), timeoutSeconds);
        return response;
    }

    @Ignore
    @Test
    public void testSignedSamlToken() throws Exception {
        Response response = createSignedSamlToken();

        System.out.println(util.pp(response));

        String formattedAssertion = util.pp(response);
        int p0 = formattedAssertion.indexOf("<ds:SignatureValue>");
        int p1 = formattedAssertion.indexOf("</ds:SignatureValue>");
        String comparableAssertion = formattedAssertion.substring(0, p0) + SIGNED_SAML_RESPONSE.substring(p0, p1) + formattedAssertion.substring(p1);
        assertEquals("SAML token differs", SIGNED_SAML_RESPONSE, comparableAssertion);
    }

    public Response createSignedSamlToken() throws Exception {
        Response response = createSamlToken();

        String certificateAliasName = "cas_certificate";
        String password = "secret";
        String keyStoreFileName = "/home/boe/tmp/code/caskeystore3.keys";
        Credential credentials = util.readCredentials(password, keyStoreFileName, certificateAliasName);

        response = util.sign(response, credentials);
        return response;
    }

    @Ignore
    @Test
    public void testHashAndSignedSamlToken() throws Exception {
        String hashAndSaml = createHashAndSignedSamlToken();
        int p0 = hashAndSaml.indexOf("<ds:SignatureValue>");
        int p1 = hashAndSaml.indexOf("</ds:SignatureValue>");
        String comparableHashAndSaml =
                hashAndSaml.substring(0, p0) + HASH_AND_SAML_RESPONSE.substring(p0, p1) + hashAndSaml.substring(p1);
        assertEquals("request digest and saml token", HASH_AND_SAML_RESPONSE, comparableHashAndSaml);
    }

    public String createHashAndSignedSamlToken() throws Exception {
        Response response = createSignedSamlToken();
        String responseString = util.toString(response);

        String request = "thisissomepayload";
        byte[] digest = util.sha256(request);
        String digestString = Base64.getEncoder().encodeToString(digest);

        String hashAndSaml = digestString + ' ' + responseString;
        return hashAndSaml;
    }

    @Ignore
    @Test
    public void testEncryptedCalvalusToken() throws Exception {
        String calvalusToken = createCalvalusToken();

        assertTrue("key+hash+saml", calvalusToken.contains(" "));

        try (FileOutputStream out = new FileOutputStream("/home/boe/tmp/code/calvalus_token.dat")) {
            out.write(calvalusToken.getBytes());
        }
    }

    private String createCalvalusToken() throws Exception {
        String hashAndSaml = createHashAndSignedSamlToken();

        File keyFile = new File("/home/boe/tmp/code/calvalus_pub.der");
        byte[] keySequence = new byte[(int) keyFile.length()];
        try (FileInputStream in = new FileInputStream(keyFile)) {
            assertEquals("public key length", keyFile.length(), in.read(keySequence));
        }
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(keySequence);
        PublicKey rsaKey = keyFactory.generatePublic(publicKeySpec);

        return util.encryptRsaAes(hashAndSaml, rsaKey);
    }

    @Ignore
    @Test
    public void testDecryptedCalvalusToken() throws Exception {
        String hashAndSaml = decryptCalvalusToken();
        int p0 = hashAndSaml.indexOf("<ds:SignatureValue>");
        int p1 = hashAndSaml.indexOf("</ds:SignatureValue>");
        String comparableHashAndSaml =
                hashAndSaml.substring(0, p0) + HASH_AND_SAML_RESPONSE.substring(p0, p1) + hashAndSaml.substring(p1);
        assertEquals("request digest and saml token", HASH_AND_SAML_RESPONSE, comparableHashAndSaml);
    }

    String decryptCalvalusToken() throws Exception {
        String calvalusToken = createCalvalusToken();
        //System.out.println(calvalusToken);

        File keyFile = new File("/home/boe/tmp/code/calvalus_priv.der");
        byte[] keySequence = new byte[(int) keyFile.length()];
        try (FileInputStream in = new FileInputStream(keyFile)) {
            in.read(keySequence);
        }
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keySequence);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PrivateKey rsaKey = keyFactory.generatePrivate(keySpec);

        return util.decryptCalvalusToken(calvalusToken, rsaKey);
    }

    @Test
    public void encryptDecryptTest() throws Exception {
        Base64.Encoder encoder = Base64.getEncoder();
        Base64.Decoder decoder = Base64.getDecoder();

        KeyGenerator keygen1 = KeyGenerator.getInstance("AES");
        keygen1.init(128);
        Key key = keygen1.generateKey();
        byte[] aesKeyBytes = key.getEncoded();
        System.out.println(new String(encoder.encode(aesKeyBytes)));

//        KeyPairGenerator keygen = KeyPairGenerator.getInstance("RSA");
//        keygen.initialize(2048);
//        KeyPair rsaKeys = keygen.genKeyPair();
//        PrivateKey privateKey = rsaKeys.getPrivate();
//        PublicKey publicKey = rsaKeys.getPublic();
//
//        try (FileOutputStream out = new FileOutputStream("/home/boe/tmp/code/test_priv.der")) {
//            out.write(privateKey.getEncoded());
//        }
//        try (FileOutputStream out = new FileOutputStream("/home/boe/tmp/code/test_pub.der")) {
//            out.write(publicKey.getEncoded());
//        }

        KeyFactory keyFactory = KeyFactory.getInstance("RSA");

        byte[] privateKeyBytes = new byte[(int) new File("/home/boe/tmp/code/calvalus_priv.der").length()];
        try (FileInputStream in = new FileInputStream("/home/boe/tmp/code/calvalus_priv.der")) {
            System.out.println(new File("/home/boe/tmp/code/calvalus_priv.der").length() + " ?= " + in.read(privateKeyBytes));
        }
        byte[] publicKeyBytes = new byte[(int) new File("/home/boe/tmp/code/calvalus_pub.der").length()];
        try (FileInputStream in = new FileInputStream("/home/boe/tmp/code/calvalus_pub.der")) {
            System.out.println(new File("/home/boe/tmp/code/calvalus_pub.der").length() + " ?= " + in.read(publicKeyBytes));
        }

        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privateKeyBytes);
        PrivateKey privateKey = keyFactory.generatePrivate(keySpec);
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publicKeyBytes);
        PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        byte[] encrypted = cipher.doFinal(aesKeyBytes);

        String encodedEncryptedAesKey = new String(encoder.encode(encrypted));
        System.out.println(encrypted.length + " " + encodedEncryptedAesKey);

        Cipher cipher2 = Cipher.getInstance("RSA");
        cipher2.init(Cipher.DECRYPT_MODE, privateKey);
        byte[] decrypted = cipher2.doFinal(decoder.decode(encodedEncryptedAesKey.getBytes()));

        System.out.println(new String(encoder.encode(decrypted)));
    }
}
