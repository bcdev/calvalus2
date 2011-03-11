package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.processing.beam.BeamOpProcessingType;
import com.bc.calvalus.processing.beam.StreamingProductReader;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.JobID;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;

import java.io.File;
import java.io.IOException;

/**
 * A production type used for generating one or more Level-2 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L2ProductionType implements ProductionType {
    private final HadoopProcessingService processingService;
    private final StagingService stagingService;
    private final L2ProcessingRequestFactory processingRequestFactory;

    L2ProductionType(HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        this.processingService = processingService;
        this.stagingService = stagingService;
        this.processingRequestFactory = new L2ProcessingRequestFactory(processingService);
    }

    @Override
    public String getName() {
        return "calvalus-level2";
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        String productionId = Production.createId(productionRequest.getProductionType());
        String productionName = createL2ProductionName(productionRequest);
        String userName = "ewa";  // todo - get user from productionRequest

        ProcessingRequest[] processingRequests = processingRequestFactory.createProcessingRequests(productionId,
                                                                                                   userName,
                                                                                                   productionRequest);
        JobID[] jobIds = new JobID[processingRequests.length];
        JobClient jobClient = processingService.getJobClient();
        BeamOpProcessingType beamOpProcessingType = new BeamOpProcessingType(jobClient);
        for (int i = 0; i < processingRequests.length; i++) {
            try {
                jobIds[i] = beamOpProcessingType.submitJob(processingRequests[i].getProcessingParameters());
            } catch (Exception e) {
                e.printStackTrace();
                throw new ProductionException("Failed to submit Hadoop job: " + e.getMessage(), e);
            }
        }

        return new Production(productionId,
                              productionName,
                              userName,
                              userName + "/" + productionId,
                              productionRequest,
                              jobIds);
    }

    @Override
    public Staging createStaging(final Production production) throws ProductionException {
        ProductionRequest productionRequest = production.getProductionRequest();

        final ProcessingRequest[] processingRequests = processingRequestFactory.createProcessingRequests(production.getId(),
                                                                                                       production.getUser(),
                                                                                                   productionRequest);
        final String jobOutputDir = processingRequests[0].getOutputDir();
        Staging staging = new Staging() {

            @Override
            public String call() throws Exception {
                float progress = 0f;
                production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, progress, ""));
                try {
                    JobClient jobClient = processingService.getJobClient();
                    Path outputPath = new Path(jobOutputDir);
                    FileSystem fileSystem = outputPath.getFileSystem(jobClient.getConf());
                    FileStatus[] seqFiles = fileSystem.listStatus(outputPath, new PathFilter() {
                        @Override
                        public boolean accept(Path path) {
                            return path.getName().endsWith(".seq");
                        }
                    });
                    File downloadDir = new File(stagingService.getStagingDir(), production.getStagingPath());
                    if (!downloadDir.exists()) {
                        downloadDir.mkdirs();
                    }
                    int index = 0;
                    for (FileStatus seqFile : seqFiles) {
                        Path seqProductPath = seqFile.getPath();
                        StreamingProductReader reader = new StreamingProductReader(seqProductPath, jobClient.getConf());
                        Product product = reader.readProductNodes(null, null);
                        String dimapProductName = seqProductPath.getName().replaceFirst(".seq", ".dim");
                        File productFile = new File(downloadDir, dimapProductName);
                        ProductIO.writeProduct(product, productFile, ProductIO.DEFAULT_FORMAT_NAME, false);
                        index++;
                        progress = (index + 1) / seqFiles.length;
                        production.setStagingStatus(new ProcessStatus(ProcessState.RUNNING, progress, ""));
                    }
                    production.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, 1.0f, ""));
                    // todo - zip or tar.gz all output DIMAPs to outputPath.getName() + ".zip" and remove outputPath.getName()
                } catch (Exception e) {
                    production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, production.getStagingStatus().getProgress(), e.getMessage()));
                    throw new ProductionException("Error: " + e.getMessage(), e);
                }
                return null;
            }
        };
        try {
            stagingService.submitStaging(staging);
        } catch (IOException e) {
            throw new ProductionException(String.format("Failed to order staging for production '%s': %s",
                                                        production.getId(), e.getMessage()), e);
        }
        return staging;
    }

    @Override
    public boolean accepts(ProductionRequest productionRequest) {
        return getName().equalsIgnoreCase(productionRequest.getProductionType());
    }

    static String createL2ProductionName(ProductionRequest productionRequest) {
        return String.format("Level 2 production using product set '%s' and L2 processor '%s'",
                             productionRequest.getProductionParameter("inputProductSetId"),
                             productionRequest.getProductionParameter("l2ProcessorName"));

    }

}
