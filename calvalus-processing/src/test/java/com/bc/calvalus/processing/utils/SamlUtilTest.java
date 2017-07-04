package com.bc.calvalus.processing.utils;

import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Test;
import org.opensaml.saml2.core.Assertion;
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
        "<saml2:Assertion Version=\"2.0\" xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\">\n" +
        "    <saml2:Issuer>cas</saml2:Issuer>\n" +
        "    <saml2:Subject>\n" +
        "        <saml2:NameID>cd_calvalus</saml2:NameID>\n" +
        "    </saml2:Subject>\n" +
        "    <saml2:Conditions NotBefore=\"2017-06-23T10:27:05.354Z\" NotOnOrAfter=\"2017-06-24T10:27:05.354Z\"/>\n" +
        "    <saml2:AuthnStatement>\n" +
        "        <saml2:AuthnContext>\n" +
        "            <saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef>\n" +
        "        </saml2:AuthnContext>\n" +
        "    </saml2:AuthnStatement>\n" +
        "    <saml2:AttributeStatement>\n" +
        "        <saml2:Attribute Name=\"groups\">\n" +
        "            <saml2:AttributeValue\n" +
        "                xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"\n" +
        "                xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalus,testproject</saml2:AttributeValue>\n" +
        "        </saml2:Attribute>\n" +
        "        <saml2:Attribute Name=\"email\">\n" +
        "            <saml2:AttributeValue\n" +
        "                xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"\n" +
        "                xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalustest@code.de</saml2:AttributeValue>\n" +
        "        </saml2:Attribute>\n" +
        "    </saml2:AttributeStatement>\n" +
        "</saml2:Assertion>\n";

    static String SIGNED_SAML_RESPONSE =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<saml2:Assertion Version=\"2.0\"\n" +
        "    xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">\n" +
        "    <saml2:Issuer>cas</saml2:Issuer>\n" +
        "    <ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">\n" +
        "        <ds:SignedInfo>\n" +
        "            <ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/>\n" +
        "            <ds:SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#dsa-sha1\"/>\n" +
        "            <ds:Reference URI=\"\">\n" +
        "                <ds:Transforms>\n" +
        "                    <ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/>\n" +
        "                    <ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\">\n" +
        "                        <ec:InclusiveNamespaces PrefixList=\"xs\" xmlns:ec=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/>\n" +
        "                    </ds:Transform>\n" +
        "                </ds:Transforms>\n" +
        "                <ds:DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"/>\n" +
        "                <ds:DigestValue>iSBjJSEPo+3Q/HPFZBkBm0ZYwEw=</ds:DigestValue>\n" +
        "            </ds:Reference>\n" +
        "        </ds:SignedInfo>\n" +
        "        IEYJZHtDALBgcqhkjOOAQDBQAwbDEQMA4GA1UEBhMHVW5rbm93bjEQMA4G\n" +
        "A1UECBMHVW5rbm93</ds:SignatureValue>\n" +
        "        <ds:KeyInfo>\n" +
        "            <ds:X509Data>\n" +
        "                <ds:X509Certificate>MIIDNTCCAvOgAwIBAgIEYJZHtDALBgcqhkjOOAQDBQAwbDEQMA4GA1UEBhMHVW5rbm93bjEQMA4G\n" +
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
        "            </ds:X509Data>\n" +
        "        </ds:KeyInfo>\n" +
        "    </ds:Signature>\n" +
        "    <saml2:Subject>\n" +
        "        <saml2:NameID>cd_calvalus</saml2:NameID>\n" +
        "    </saml2:Subject>\n" +
        "    <saml2:Conditions NotBefore=\"2017-06-23T10:27:05.354Z\" NotOnOrAfter=\"2017-06-24T10:27:05.354Z\"/>\n" +
        "    <saml2:AuthnStatement>\n" +
        "        <saml2:AuthnContext>\n" +
        "            <saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef>\n" +
        "        </saml2:AuthnContext>\n" +
        "    </saml2:AuthnStatement>\n" +
        "    <saml2:AttributeStatement>\n" +
        "        <saml2:Attribute Name=\"groups\">\n" +
        "            <saml2:AttributeValue\n" +
        "                xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalus,testproject</saml2:AttributeValue>\n" +
        "        </saml2:Attribute>\n" +
        "        <saml2:Attribute Name=\"email\">\n" +
        "            <saml2:AttributeValue\n" +
        "                xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalustest@code.de</saml2:AttributeValue>\n" +
        "        </saml2:Attribute>\n" +
        "    </saml2:AttributeStatement>\n" +
        "</saml2:Assertion>\n";

    public static final String HASH_AND_SAML_RESPONSE = "8eWW6PfyhxTVhEfPkRiWZ3BHBjN+q0t043IoIrBYyuM= <?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<saml2:Assertion Version=\"2.0\" xmlns:saml2=\"urn:oasis:names:tc:SAML:2.0:assertion\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"><saml2:Issuer>cas</saml2:Issuer><ds:Signature xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\"><ds:SignedInfo><ds:CanonicalizationMethod Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/><ds:SignatureMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#dsa-sha1\"/><ds:Reference URI=\"\"><ds:Transforms><ds:Transform Algorithm=\"http://www.w3.org/2000/09/xmldsig#enveloped-signature\"/><ds:Transform Algorithm=\"http://www.w3.org/2001/10/xml-exc-c14n#\"><ec:InclusiveNamespaces PrefixList=\"xs\" xmlns:ec=\"http://www.w3.org/2001/10/xml-exc-c14n#\"/></ds:Transform></ds:Transforms><ds:DigestMethod Algorithm=\"http://www.w3.org/2000/09/xmldsig#sha1\"/><ds:DigestValue>iSBjJSEPo+3Q/HPFZBkBm0ZYwEw=</ds:DigestValue></ds:Reference></ds:SignedInfo><ds:SignatureValue>TO3J92WEr5P1fWJpx538BPkPqECLuFo/mcYCuxIzHK5mnEqHEC44Uw==</ds:SignatureValue><ds:KeyInfo><ds:X509Data><ds:X509Certificate>MIIDNTCCAvOgAwIBAgIEYJZHtDALBgcqhkjOOAQDBQAwbDEQMA4GA1UEBhMHVW5rbm93bjEQMA4G\n" +
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
            "P8VfV8cCFClquisEfDHmF2u6Kl3OMWs4HLD9</ds:X509Certificate></ds:X509Data></ds:KeyInfo></ds:Signature><saml2:Subject><saml2:NameID>cd_calvalus</saml2:NameID></saml2:Subject><saml2:Conditions NotBefore=\"2017-06-23T10:27:05.354Z\" NotOnOrAfter=\"2017-06-24T10:27:05.354Z\"/><saml2:AuthnStatement><saml2:AuthnContext><saml2:AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:Password</saml2:AuthnContextClassRef></saml2:AuthnContext></saml2:AuthnStatement><saml2:AttributeStatement><saml2:Attribute Name=\"groups\"><saml2:AttributeValue xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalus,testproject</saml2:AttributeValue></saml2:Attribute><saml2:Attribute Name=\"email\"><saml2:AttributeValue xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"xs:string\">calvalustest@code.de</saml2:AttributeValue></saml2:Attribute></saml2:AttributeStatement></saml2:Assertion>";

    SamlUtil util;
    public SamlUtilTest() throws NoSuchAlgorithmException, ConfigurationException {
        util = new SamlUtil();
    }

    public Assertion createSamlToken() throws Exception {
        String issuer = "cas";
        String subject = "cd_calvalus";
        Map<String,String> attributes = new HashMap<>();
        attributes.put("groups", "calvalus,testproject");
        attributes.put("email", "calvalustest@code.de");
        int timeoutSeconds = 60*60*24;

        Assertion assertion = util.build(issuer, subject, attributes, new DateTime("2017-06-23T10:27:05.354Z"), timeoutSeconds);
        return assertion;
    }

    public Assertion createSignedSamlToken() throws Exception {
        Assertion assertion = createSamlToken();

        //String certificateAliasName = "cas_certificate";
        //String password = "secret";
        //String keyStoreFileName = "/home/boe/tmp/code/caskeystore3.keys";
        //Credential credentials = util.readCredentials(password, keyStoreFileName, certificateAliasName);
        Credential credentials = util.readCredentials2("/home/boe/tmp/code/cas_priv.der", "/home/boe/tmp/code/cas_cert.der");

        assertion = util.sign(assertion, credentials);
        return assertion;
    }

    public String createHashAndSignedSamlToken() throws Exception {
        Assertion assertion = createSignedSamlToken();
        String assertionString = util.toString(assertion);

        String request = "thisissomepayload";
        byte[] digest = util.sha256(request);
        String digestString = Base64.getEncoder().encodeToString(digest);

        String hashAndSaml = digestString + ' ' + assertionString;
        return hashAndSaml;
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
    public void testSamlToken() throws Exception {
        Assertion assertion = createSamlToken();
        String formattedAssertion = util.pp(assertion);
        assertEquals("SAML token differs", SIMPLE_SAML_RESPONSE, formattedAssertion);
    }

    @Ignore
    @Test
    public void testSignedSamlToken() throws Exception {
        Assertion assertion = createSignedSamlToken();

        System.out.println(util.pp(assertion));

        String formattedAssertion = util.pp(assertion);
        int p0 = formattedAssertion.indexOf("<ds:SignatureValue>");
        int p1 = formattedAssertion.indexOf("</ds:SignatureValue>");
        String comparableAssertion = formattedAssertion.substring(0, p0) + SIGNED_SAML_RESPONSE.substring(p0, p1) + formattedAssertion.substring(p1);
        assertEquals("SAML token differs", SIGNED_SAML_RESPONSE, comparableAssertion);
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

    @Ignore
    @Test
    public void testEncryptedCalvalusToken() throws Exception {
        String calvalusToken = createCalvalusToken();

        assertTrue("key+hash+saml", calvalusToken.contains(" "));

        try (FileOutputStream out = new FileOutputStream("/home/boe/tmp/code/calvalus_token.dat")) {
            out.write(calvalusToken.getBytes());
        }
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

    @Ignore
    @Test
    public void testCheckSignature() throws Exception {
        String hashAndSaml = decryptCalvalusToken();
        String samlString = hashAndSaml.substring(hashAndSaml.indexOf(' ') + 1);

        //samlString = samlString.replace("cd_calvalus", "martin");

//        String certificateAliasName = "cas_certificate";
//        String password = "secret";
//        String keyStoreFileName = "/home/boe/tmp/code/caskeystore3.keys";
//        Credential credentials = util.readCredentials(password, keyStoreFileName, certificateAliasName);
        Credential credentials = util.readCredentials2("/home/boe/tmp/code/cas_priv.der", "/home/boe/tmp/code/cas_cert.pem");

        boolean isValid = util.validate(samlString, credentials);

        assertTrue("SAML token signature is valid", isValid);
    }

    @Ignore
    @Test
    public void encryptDecryptTest() throws Exception {

        String clear = "messagefromc____";
        System.out.println(clear);

        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        byte[] privateKeyBytes = new byte[(int) new File("/home/boe/tmp/code/calvalus_priv.der").length()];
        try (FileInputStream in = new FileInputStream("/home/boe/tmp/code/calvalus_priv.der")) {
            in.read(privateKeyBytes);
        }
        byte[] publicKeyBytes = new byte[(int) new File("/home/boe/tmp/code/calvalus_pub.der").length()];
        try (FileInputStream in = new FileInputStream("/home/boe/tmp/code/calvalus_pub.der")) {
            in.read(publicKeyBytes);
        }

        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privateKeyBytes);
        PrivateKey privateKey = keyFactory.generatePrivate(keySpec);
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publicKeyBytes);
        PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

        Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        byte[] encj = cipher.doFinal(clear.getBytes("UTF-8"));

        try (FileOutputStream out = new FileOutputStream("/home/boe/tmp/code/encj.dat")) {
            out.write(encj);
        }

        Cipher cipher2 = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
        cipher2.init(Cipher.DECRYPT_MODE, privateKey);
        byte[] decj = cipher2.doFinal(encj);

        System.out.println(new String(decj, "UTF-8"));

        byte[] encc = new byte[(int) new File("/home/boe/tmp/code/encc.dat").length()];
        int encclen;
        try (FileInputStream in = new FileInputStream("/home/boe/tmp/code/encc.dat")) {
            encclen = in.read(encc);
        }

        Cipher cipher3 = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
        cipher3.init(Cipher.DECRYPT_MODE, privateKey);
        byte[] decc = cipher3.doFinal(encc);

        System.out.println(new String(decc, "UTF-8"));

    }
}
