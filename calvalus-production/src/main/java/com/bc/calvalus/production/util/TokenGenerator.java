package com.bc.calvalus.production.util;

import com.bc.calvalus.processing.hadoop.HadoopJobHook;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class TokenGenerator extends SamlUtil implements HadoopJobHook {

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

    private final String samlToken;

    public TokenGenerator(String publicKey, String samlToken) {
        super(publicKey);
        this.samlToken = samlToken;
    }

    @Override
    public void beforeSubmit(Job job) {
        job.getConfiguration().set("calvalus.token", calvalusTokenOf(job.getConfiguration()));
    }

    private String calvalusTokenOf(Configuration jobParameters) {
        try {
            String characterizingParameters = characterizingParametersOf(jobParameters);
            String requestHashString = hashStringOf(characterizingParameters);
            String hashAndSaml = requestHashString + ' ' + samlToken;
            return encryptRsaAes(hashAndSaml, getCalvalusPublicKey());
        } catch (NoSuchPaddingException | InvalidAlgorithmParameterException |
                NoSuchAlgorithmException | IOException | BadPaddingException | IllegalBlockSizeException |
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

}
