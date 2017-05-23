package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.store.MemoryProductionStore;
import org.apache.commons.codec.binary.Hex;
import org.junit.Before;
import org.junit.Test;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProductionServiceImplTest {

    private ProductionServiceImpl productionServiceUnderTest;

    private TestFileSystemService filesystemServiceMock;
    private TestProcessingService processingServiceMock;
    private TestStagingService stagingServiceMock;
    private MemoryProductionStore productionStoreMock;
    private TestProductionType productionTypeMock;

    @Before
    public void setUp() throws Exception {
        filesystemServiceMock = new TestFileSystemService();
        processingServiceMock = new TestProcessingService();
        stagingServiceMock = new TestStagingService();
        productionTypeMock = new TestProductionType(processingServiceMock,
                stagingServiceMock);
        productionStoreMock = new MemoryProductionStore();
        productionServiceUnderTest = new ProductionServiceImpl(
                false,
                filesystemServiceMock,
                processingServiceMock,
                stagingServiceMock,
                productionStoreMock,
                productionTypeMock);
    }

    @Test
    public void testOrderProduction() throws ProductionException {

        ProductionRequest request = new ProductionRequest("test", "ewa");
        ProductionResponse productionResponse = productionServiceUnderTest.orderProduction(request);
        assertNotNull(productionResponse);
        assertNotNull(productionResponse.getProduction());
        assertEquals("id_1", productionResponse.getProduction().getId());
        assertEquals("name_1", productionResponse.getProduction().getName());
        assertNotNull(productionResponse.getProduction().getJobIds());
        assertEquals(2, productionResponse.getProduction().getJobIds().length);
        assertEquals("job_1_1", productionResponse.getProduction().getJobIds()[0]);
        assertEquals("job_1_2", productionResponse.getProduction().getJobIds()[1]);
        assertNotNull(productionResponse.getProduction().getProductionRequest());
        assertEquals(request, productionResponse.getProduction().getProductionRequest());
        assertEquals("stagingPath_1", productionResponse.getProduction().getStagingPath());
    }

    @Test
    public void testOrderUnknownProductionType() {
        try {
            productionServiceUnderTest.orderProduction(new ProductionRequest("erase-hdfs", "devil"));
            fail("ProductionException expected, since 'erase-hdfs' is not a valid production type");
        } catch (ProductionException e) {
            // expected
        }
    }

    @Test
    public void testGetProductions() throws ProductionException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(3, productions.length);
        assertEquals("id_1", productions[0].getId());
        assertEquals("id_2", productions[1].getId());
        assertEquals("id_3", productions[2].getId());

        // Make sure data store is used
        assertSame(productions[0], productionStoreMock.getProduction("id_1"));
        assertSame(productions[1], productionStoreMock.getProduction("id_2"));
        assertSame(productions[2], productionStoreMock.getProduction("id_3"));
        assertNull(productionStoreMock.getProduction("id_x"));
    }


    @Test
    public void testGetProductionStatusPropagation() throws ProductionException, IOException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(3, productions.length);

        assertEquals(ProcessStatus.UNKNOWN, productions[0].getProcessingStatus());
        assertEquals(ProcessStatus.UNKNOWN, productions[1].getProcessingStatus());
        assertEquals(ProcessStatus.UNKNOWN, productions[2].getProcessingStatus());

        processingServiceMock.setJobStatus("job_1_1", new ProcessStatus(ProcessState.RUNNING, 0.2f));
        processingServiceMock.setJobStatus("job_1_2", new ProcessStatus(ProcessState.RUNNING, 0.4f));
        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.SCHEDULED));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.RUNNING, 0.8f));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses("ewa");

        assertEquals(new ProcessStatus(ProcessState.RUNNING, 0.3f), productions[0].getProcessingStatus());
        assertEquals(new ProcessStatus(ProcessState.COMPLETED), productions[1].getProcessingStatus());
        assertEquals(new ProcessStatus(ProcessState.RUNNING, 0.4f), productions[2].getProcessingStatus());
    }


    @Test
    public void testDeleteProductions() throws ProductionException, IOException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));

        productionServiceUnderTest.deleteProductions("id_2", "id_4");

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(5, productions.length);// cannot delete, because its status is not 'done'

        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.RUNNING));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.RUNNING));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses("ewa");

        productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(4, productions.length);// now can delete id_2, because its status is not 'done'

        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.COMPLETED));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses("ewa");

        productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(3, productions.length);// now can delete id_4, because its status is not 'done'
    }

    @Test
    public void testDeleteUnknownProduction() {
        try {
            productionServiceUnderTest.deleteProductions("id_45");
            fail("ProductionException expected, because we don't have production 'id_45'");
        } catch (ProductionException e) {
            // ok
        }
    }

    @Test
    public void testCancelProductions() throws ProductionException, IOException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));

        processingServiceMock.setJobStatus("job_1_1", new ProcessStatus(ProcessState.SCHEDULED));
        processingServiceMock.setJobStatus("job_1_2", new ProcessStatus(ProcessState.RUNNING));
        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_1", new ProcessStatus(ProcessState.RUNNING));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.RUNNING));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.RUNNING));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses("ewa");

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(4, productions.length);
        assertEquals(ProcessState.RUNNING, productions[0].getProcessingStatus().getState());
        assertEquals(ProcessState.COMPLETED, productions[1].getProcessingStatus().getState());
        assertEquals(ProcessState.RUNNING, productions[2].getProcessingStatus().getState());
        assertEquals(ProcessState.RUNNING, productions[3].getProcessingStatus().getState());

        productionServiceUnderTest.cancelProductions("id_1", "id_2", "id_4");

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses("ewa");

        productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(4, productions.length);
        assertEquals(ProcessState.CANCELLED, productions[0].getProcessingStatus().getState());
        assertEquals(ProcessState.COMPLETED, productions[1].getProcessingStatus().getState());
        assertEquals(ProcessState.RUNNING, productions[2].getProcessingStatus().getState());
        assertEquals(ProcessState.CANCELLED, productions[3].getProcessingStatus().getState());
    }

    @Test
    public void testCancelUnknownProduction() {
        try {
            productionServiceUnderTest.cancelProductions("id_25");
            fail("ProductionException expected, because we don't have production 'id_25'");
        } catch (ProductionException e) {
            // ok
        }
    }

    @Test
    public void testStageProduction() throws ProductionException, IOException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa", "autoStaging", "false"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa", "autoStaging", "false"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa", "autoStaging", "true"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa", "autoStaging", "true"));

        processingServiceMock.setJobStatus("job_1_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_1_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.COMPLETED));

        assertTrue(stagingServiceMock.getStagings().isEmpty());

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses("ewa");

        assertEquals(2, stagingServiceMock.getStagings().size());

        productionServiceUnderTest.stageProductions("id_1", "id_2");

        assertEquals(4, stagingServiceMock.getStagings().size());
    }

    @Test
    public void testStageUnknownProduction() {
        try {
            productionServiceUnderTest.stageProductions("id_98");
            fail("ProductionException expected, because we don't have production 'id_98'");
        } catch (ProductionException e) {
            // ok
        }
    }

    @Test
    public void testClose() throws Exception {
        assertEquals(false, stagingServiceMock.isClosed());
        assertEquals(false, processingServiceMock.isClosed());
        assertEquals(false, productionStoreMock.isClosed());
        productionServiceUnderTest.close();
        assertEquals(true, stagingServiceMock.isClosed());
        assertEquals(true, processingServiceMock.isClosed());
        assertEquals(true, productionStoreMock.isClosed());
    }

    @Test
    public void testRegularExpression() throws Exception {
        Pattern pattern = Pattern.compile("(.*)@(.*):(.*)");
        Matcher matcher = pattern.matcher("freshmon@freshmon-csw:/data/postprocessing/entries");
        assertEquals(3, matcher.groupCount());
        assertEquals(true, matcher.find());
        assertEquals("freshmon@freshmon-csw:/data/postprocessing/entries", matcher.group(0));
        assertEquals("freshmon", matcher.group(1));
        assertEquals("freshmon-csw", matcher.group(2));
        assertEquals("/data/postprocessing/entries", matcher.group(3));

    }

    @Test
    public void testSubmitRequestWithSamlResponse() throws Exception {
        String samlToken = "<?xml version=\"1.0\"?>\n" +
                "<samlp:Response ID=\"_rst456\" InResponseTo=\"_abc123\" IssueInstant=\"2014-09-17T21:06:32\" Version=\"2.0\">\n" +
                "  <samlp:Status>\n" +
                "    <samlp:StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\"/>\n" +
                "  </samlp:Status>\n" +
                "  <saml:Assertion ID=\"_xyz888\" IssueInstant=\"2014-09-17T21:06:32\" Version=\"2.0\">\n" +
                "    <saml:AttributeStatement>\n" +
                "      <saml:Attribute Name=\"email\">\n" +
                "        <saml:AttributeValue>alice@acme-corp.biz</saml:AttributeValue>\n" +
                "      </saml:Attribute>\n" +
                "\t  <saml:Attribute Name=\"uid\" NameFormat=\"urn:oasis:names:tc:SAML:2.0:attrname-format:basic\">\n" +
                "        <saml:AttributeValue xsi:type=\"xs:string\">test</saml:AttributeValue>\n" +
                "      </saml:Attribute>\n" +
                "    </saml:AttributeStatement>\n" +
                "  </saml:Assertion>\n" +
                "</samlp:Response>";


        ProductionService productionServiceUnderTest = new ProductionServiceImpl(
                true,
                filesystemServiceMock,
                processingServiceMock,
                stagingServiceMock,
                productionStoreMock,
                new TestProductionType(processingServiceMock, stagingServiceMock) {
                    @Override
                    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
                        // receiving a public-key encrypted SAML token
                        // ("the CAS server should encrypt the resulting SAML token using the user's public key")

                        assertNotNull(productionRequest);
                        try {
                            productionRequest.ensureParameterSet("authToken");
                        } catch (ProductionException e) {
                            fail(e.getMessage());
                        }
                        try {
                            String authToken = productionRequest.getParameter("authToken", false);
                            String decryptedAuthToken = null;
                            decryptedAuthToken = decrypt(authToken);

                            String[] splittedAuthToken = decryptedAuthToken.split(ProductionServiceImpl.SEPARATOR);
                            String samlTokenFromProductionService = splittedAuthToken[0];
                            String userName = splittedAuthToken[1];
                            String regionName = splittedAuthToken[2];
                            String productionType = splittedAuthToken[3];
                            assertEquals(decrypt(samlTokenFromProductionService), samlToken);
                            assertEquals("some_user", userName);
                            assertEquals("Southern Bielefeld", regionName);
                            assertEquals("test", productionType);
                        } catch (Exception e) {
                            throw new ProductionException(e);
                        }
                        return super.createProduction(productionRequest);
                    }
                });

        String encryptedSamlToken = ProductionServiceImpl.encrypt(samlToken);
        ProductionRequest request = new ProductionRequest("test", "some_user", "regionName", "Southern Bielefeld");
        ProductionResponse productionResponse = productionServiceUnderTest.orderProduction(request, encryptedSamlToken);

        assertNotNull(productionResponse);
        ProductionRequest extendedProductionRequest = productionResponse.getProduction().getProductionRequest();
        assertNull(extendedProductionRequest.getString("authToken", null));
    }

    public static String decrypt(String encrypted) throws Exception {
        Cipher decrypt = Cipher.getInstance("RSA");
        decrypt.init(Cipher.DECRYPT_MODE, getPrivateKey());
        byte[] bytes = Hex.decodeHex(encrypted.toCharArray());

        byte[] decrypted = blockCipher(decrypt, bytes, Cipher.DECRYPT_MODE);

        return new String(decrypted, "UTF-8");
    }

    private static PrivateKey getPrivateKey() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        byte[] keyBytes = Files.readAllBytes(Paths.get("d:\\workspace\\code\\testkey\\private_key.der"));

        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(spec);
    }

    private static byte[] blockCipher(Cipher cipher, byte[] bytes, int mode) throws IllegalBlockSizeException, BadPaddingException {
        // string initialize 2 buffers.
        // scrambled will hold intermediate results
        byte[] scrambled;

        // toReturn will hold the total result
        byte[] toReturn = new byte[0];
        // if we encrypt we use 100 byte long blocks. Decryption requires 128 byte long blocks (because of RSA)
        int length = (mode == Cipher.ENCRYPT_MODE) ? 100 : 128;

        // another buffer. this one will hold the bytes that have to be modified in this step
        byte[] buffer = new byte[length];

        for (int i = 0; i < bytes.length; i++) {

            // if we filled our buffer array we have our block ready for de- or encryption
            if ((i > 0) && (i % length == 0)) {
                //execute the operation
                scrambled = cipher.doFinal(buffer);
                // add the result to our total result.
                toReturn = append(toReturn, scrambled);
                // here we calculate the length of the next buffer required
                int newlength = length;

                // if newlength would be longer than remaining bytes in the bytes array we shorten it.
                if (i + length > bytes.length) {
                    newlength = bytes.length - i;
                }
                // clean the buffer array
                buffer = new byte[newlength];
            }
            // copy byte into our buffer.
            buffer[i % length] = bytes[i];
        }

        // this step is needed if we had a trailing buffer. should only happen when encrypting.
        // example: we encrypt 110 bytes. 100 bytes per run means we "forgot" the last 10 bytes. they are in the buffer array
        scrambled = cipher.doFinal(buffer);

        // final step before we can return the modified data.
        toReturn = append(toReturn, scrambled);

        return toReturn;
    }

    private static byte[] append(byte[] prefix, byte[] suffix) {
        byte[] toReturn = new byte[prefix.length + suffix.length];
        System.arraycopy(prefix, 0, toReturn, 0, prefix.length);
        System.arraycopy(suffix, 0, toReturn, prefix.length, suffix.length);
        return toReturn;
    }

}