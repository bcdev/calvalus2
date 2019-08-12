package com.bc.calvalus.production.util;

import org.apache.xml.security.Init;
import org.apache.xml.security.encryption.XMLCipher;
import org.apache.xml.security.encryption.XMLEncryptionException;
import org.apache.xml.security.utils.EncryptionConstants;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.jdom2.Element;
import org.jdom2.input.DOMBuilder;
import org.jdom2.output.DOMOutputter;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.crypto.Cipher;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class CasUtil {
    private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    boolean quiet;

    public CasUtil(boolean quiet) {
        this.quiet = quiet;
    }

    private void say(String message) {
        if (!quiet) {
            System.out.println(message);
        }
    }


    public String fetchSamlToken2(Map<Object, Object> config, String userName) throws IOException, GeneralSecurityException, SAXException, ParserConfigurationException, XMLEncryptionException, org.jdom2.JDOMException {
        Init.init();
        String casVersion = (String) config.getOrDefault("calvalus.cas.version", "5");
        String casUrl = (String) config.getOrDefault("calvalus.cas.url", "https://sso.eoc.dlr.de/cas-codede");
        String portalUrl = (String) config.getOrDefault("calvalus.portal.url", "https://processing.code-de.org/calvalus.jsp");
        String privateKeyPath = (String) config.get("calvalus.crypt.samlkey-private-key");
        if (privateKeyPath == null) {
            throw new IllegalStateException("No entry for calvalus.crypt.samlkey-private-key found in Calvalus config.");
        }
        if (!casVersion.equals("5") && !casVersion.equals("4")) {
            throw new IllegalStateException("calvalus.cas.version has to be set to either 4 or 5.");
        }
        say(String.format("Fetching SAML token from %s, CAS version %s", casUrl, casVersion));
        String tgt = fetchTgt(casUrl, casVersion, userName);
        String samlToken = fetchSamlToken(tgt, casUrl, portalUrl, casVersion);
        PrivateKey privateSamlKey = readPrivateDerKey(privateKeyPath);
        Document document = parseXml(samlToken);
        document = decipher(privateSamlKey, document);
        document = fixRootNode(document);
        return getStringFromDoc(document);
    }

    public String fetchSamlToken(Map<String, String> config, String userName) throws IOException, GeneralSecurityException, SAXException, ParserConfigurationException, XMLEncryptionException, org.jdom2.JDOMException {
        Init.init();
        String casVersion = config.getOrDefault("calvalus.cas.version", "5");
        String casUrl = config.getOrDefault("calvalus.cas.url", "https://sso.eoc.dlr.de/cas-codede");
        String portalUrl = config.getOrDefault("calvalus.portal.url", "https://processing.code-de.org/calvalus.jsp");
        String privateKeyPath = config.get("calvalus.crypt.samlkey-private-key");
        if (privateKeyPath == null) {
            throw new IllegalStateException("No entry for calvalus.crypt.samlkey-private-key found in Calvalus config.");
        }
        if (!casVersion.equals("5") && !casVersion.equals("4")) {
            throw new IllegalStateException("calvalus.cas.version has to be set to either 4 or 5.");
        }
        say(String.format("Fetching SAML token from %s, CAS version %s", casUrl, casVersion));
        String tgt = fetchTgt(casUrl, casVersion, userName);
        String samlToken = fetchSamlToken(tgt, casUrl, portalUrl, casVersion);
        PrivateKey privateSamlKey = readPrivateDerKey(privateKeyPath);
        Document document = parseXml(samlToken);
        document = decipher(privateSamlKey, document);
        document = fixRootNode(document);
        return getStringFromDoc(document);
    }

    private String fetchSamlToken(String tgt, final String casUrl, String portalUrl, String casVersion) throws IOException {
        String serviceParameterName = casVersion.equals("4") ? "serviceUrl" : "service";
        String urlString = casUrl + "/samlCreate2?" + serviceParameterName + "=" + portalUrl;
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestProperty("charset", "utf-8");
        conn.setRequestProperty("Cookie", "CASTGC=" + tgt);
        conn.setUseCaches(false);
        StringBuilder saml = new StringBuilder();
        try (InputStream in = conn.getInputStream()) {
            int c;
            while ((c = in.read()) > 0) {
                saml.append((char) c);
            }
        }

        return saml.toString();
    }

    private static String getStringFromDoc(Document doc) {
        try {
            DOMSource domSource = new DOMSource(doc);
            StringWriter writer = new StringWriter();
            StreamResult result = new StreamResult(writer);
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.transform(domSource, result);
            writer.flush();
            return writer.toString();
        } catch (TransformerException ex) {
            throw new IllegalArgumentException("Unable to parse SAML token", ex);
        }
    }

    private static Document parseXml(String xml) throws SAXException, IOException, ParserConfigurationException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputStream inputStream = new ByteArrayInputStream(xml.getBytes());
        return db.parse(inputStream);
    }

    public static Document fixRootNode(Document samlToken) throws org.jdom2.JDOMException {
        DOMBuilder builder = new DOMBuilder();
        org.jdom2.Document jDomDoc = builder.build(samlToken);
        Element assertionElement = jDomDoc.getRootElement().getChildren().get(0);
        jDomDoc.detachRootElement();
        assertionElement.detach();
        jDomDoc.setRootElement(assertionElement);
        DOMOutputter outputter = new DOMOutputter();
        return outputter.output(jDomDoc);
    }

    private String fetchTgt(String casUrl, String casVersion, String userName) throws IOException, GeneralSecurityException {
        byte[] encryptedSecret = createEncryptedSecret(userName);
        String urlParameters = "client_name=PKEY&userid=" + userName + "&secret=" + new String(encryptedSecret);
        byte[] postData = urlParameters.getBytes(StandardCharsets.UTF_8);
        int postDataLength = postData.length;
        URL url = new URL(casUrl + "/login");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestProperty("charset", "utf-8");
        conn.setRequestProperty("Content-Length", Integer.toString(postDataLength));
        conn.setUseCaches(false);
        try (DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
            wr.write(postData);
        }
        Map<String, List<String>> headerFields = conn.getHeaderFields();
        if (headerFields == null) {
            throw new GeneralSecurityException("Could not retrieve TGT from URL " + casUrl);
        }
        List<String> setCookieFields = headerFields.get("Set-Cookie");
        if (setCookieFields == null || (casVersion.equals("4") && setCookieFields.size() < 2)) {
            return logError(conn);
        }
        int cookieIndex = casVersion.equals("4") ? 1 : 0;
        String setCookie = setCookieFields.get(cookieIndex);
        String tgtPart1 = setCookie.split(";")[0];

        return tgtPart1.split("=")[1];
    }

    private static String logError(HttpURLConnection conn) throws IOException {
        InputStream in = conn.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        StringBuilder result = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }
        throw new IOException("Fetching TGT failed. Reply from CAS server:\n" + result.toString());
    }

    private static PrivateKey readPrivateDerKey(String filename) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, NoSuchProviderException {
        byte[] privKeyByteArray = Files.readAllBytes(Paths.get(filename));
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privKeyByteArray);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(keySpec);
    }

    private static PrivateKey readPrivatePemKey(String filename) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, NoSuchProviderException {
        Security.addProvider(new BouncyCastleProvider());
        try (final PemReader pr = new PemReader(new FileReader(filename))) {
            final PemObject po = pr.readPemObject();

            final KeySpec keySpec = new PKCS8EncodedKeySpec(po.getContent());
            final KeyFactory keyFact = KeyFactory.getInstance("RSA", "BC");
            return keyFact.generatePrivate(keySpec);
        }
    }

    private static Document decipher(PrivateKey myPrivKey, Document document) throws XMLEncryptionException {
        org.w3c.dom.Element encryptedDataElement = (org.w3c.dom.Element) document.getElementsByTagNameNS(EncryptionConstants.EncryptionSpecNS, EncryptionConstants._TAG_ENCRYPTEDDATA).item(0);
        XMLCipher xmlCipher = XMLCipher.getInstance();
        xmlCipher.init(XMLCipher.DECRYPT_MODE, null);
        xmlCipher.setKEK(myPrivKey);
        try {
            return xmlCipher.doFinal(document, encryptedDataElement);
        } catch (Exception e) {
            throw new XMLEncryptionException("", e);
        }
    }

    private byte[] createEncryptedSecret(String userId) throws IOException, GeneralSecurityException {
        PrivateKey privateKey = readPrivatePemKey("/home/" + userId + "/.ssh/id_rsa");
        final Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, privateKey);
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        String token = userId + '\n' + df.format(new Date()) + "\n47110815";
        byte[] bytes = cipher.doFinal(token.getBytes());
        return Base64.getUrlEncoder().encode(bytes);
    }

}
