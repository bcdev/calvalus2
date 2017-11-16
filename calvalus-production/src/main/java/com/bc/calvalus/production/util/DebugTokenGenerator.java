package com.bc.calvalus.production.util;

import com.bc.calvalus.processing.hadoop.HadoopJobHook;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.opensaml.saml2.core.Assertion;
import org.opensaml.xml.ConfigurationException;
import org.opensaml.xml.io.MarshallingException;
import org.opensaml.xml.security.credential.Credential;
import org.opensaml.xml.signature.SignatureException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class DebugTokenGenerator extends SamlUtil implements HadoopJobHook {

    private static final String[] CALVALUS_HASHABLE_PARAMETERS = {
            "calvalus.productionType", //
            "mapreduce.job.name", // "productionName",
            "calvalus.bundles", // "processorBundleName", "processorBundleVersion",
            "calvalus.l2.operator", // "processorName",
            "calvalus.input.pathPatterns", // "inputPath",
            "calvalus.input.dateRanges", // "minDate", "maxDate",
            "calvalus.regionGeometry", // "regionWKT",
            "calvalus.l2.parameters", // "processorParameters",
            "calvalus.l3.parameters",
            "calvalus.ql.parameters",
            "calvalus.output.dir",
            "mapreduce.job.queuename", // "calvalus.hadoop.mapreduce.job.queuename"
    };

    public static final String CALVALUS_ISSUER = "calvalus";

    private final String userName;
    private final Credential debugCredential;

    public DebugTokenGenerator(Map<String, String> config, String userName) {
        super(config);
        this.userName = userName;
        try {
            debugCredential = readCredentials(config.get("calvalus.crypt.debug-private-key"),
                                              config.get("calvalus.crypt.debug-certificate"));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException | CertificateException e) {
            throw new RuntimeException("exception reading keys and certificates", e);
        }
    }

    @Override
    public void beforeSubmit(Job job) {
        job.getConfiguration().set("calvalus.token", calvalusTokenOf(job.getConfiguration(), userName));
    }

    private String calvalusTokenOf(Configuration jobParameters, String userName) {
        try {
            String characterizingParameters = characterizingParametersOf(jobParameters);
            //System.out.println(characterizingParameters);
            String requestHashString = hashStringOf(characterizingParameters);
            //System.out.println("base64(hash(request))=" + requestHashString);
            String assertionString = signedAssertionStringOf(userName, debugCredential);
            //System.out.println(assertionString);
            String hashAndSaml = requestHashString + ' ' + assertionString;
            String debugToken = encryptRsaAes(hashAndSaml, getCalvalusPublicKey());
            return debugToken;

        } catch (ConfigurationException | org.opensaml.xml.security.SecurityException |SignatureException |
                MarshallingException |NoSuchPaddingException |InvalidAlgorithmParameterException |
                NoSuchAlgorithmException |IOException|BadPaddingException |IllegalBlockSizeException |
                InvalidKeyException e) {
            throw new RuntimeException("exception creating calvalus token", e);
        }
    }

    private String characterizingParametersOf(Configuration jobParameters) {
        StringBuilder accu = new StringBuilder();
        for (String key : CALVALUS_HASHABLE_PARAMETERS) {
            String value = jobParameters.get(key);
            if (value != null) {
                accu.append(key);
                accu.append('=');
                accu.append(value);
                accu.append('\n');
            }
        }
        return accu.toString();
    }

    private String hashStringOf(String requestHashable) throws UnsupportedEncodingException {
        byte[] requestHash = sha256(requestHashable);
        return Base64.getEncoder().encodeToString(requestHash);
    }

    private String signedAssertionStringOf(String userName, Credential credential) throws ConfigurationException, org.opensaml.xml.security.SecurityException, MarshallingException, SignatureException {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("uid", userName);
        attributes.put("groups", "calvalus");
        int timeoutSeconds = 60 * 60 * 24;
        Assertion assertion = build(CALVALUS_ISSUER, userName, attributes, DateTime.now(DateTimeZone.UTC), timeoutSeconds);
        assertion = sign(assertion, credential);
        return toString(assertion);
    }
}
