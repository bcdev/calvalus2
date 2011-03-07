package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.StreamingProductReader;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobID;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * A production type used for generating one or more Level-2 products.
 * @author MarcoZ
 * @author Norman
*/
public class L2ProductionType implements ProductionType {
    private WpsXmlGenerator wpsXmlGenerator;
    private ExecutorService stagingService;
    private Logger logger;
    private File localStagingDir;
    private JobClient jobClient;

    L2ProductionType(JobClient jobClient, Logger logger, File localStagingDir) throws ProductionException {
        this.logger = logger;
        this.localStagingDir = localStagingDir;
        this.jobClient = jobClient;
        wpsXmlGenerator = new WpsXmlGenerator();
        stagingService = Executors.newFixedThreadPool(3); // todo - make numThreads configurable
    }


    @Override
    public String getName() {
        return "calvalus-level2";
    }

    @Override
    public HadoopProduction createProduction(ProductionRequest pdr) throws ProductionException {
        throw new ProductionException("L2 production not implemented yet.");
    }

    @Override
    public void stageProduction(HadoopProduction p) throws ProductionException {

        JobID jobId = p.getJobId();
        // todo - spawn separate thread, use StagingRequest/StagingResponse/WorkStatus
        try {
            RunningJob job = jobClient.getJob(org.apache.hadoop.mapred.JobID.downgrade(jobId));
            String jobFile = job.getJobFile();
            // System.out.printf("jobFile = %n%s%n", jobFile);
            Configuration configuration = new Configuration(jobClient.getConf());
            configuration.addResource(new Path(jobFile));

            String jobOutputDir = configuration.get("mapred.output.dir");
            // System.out.println("mapred.output.dir = " + jobOutputDir);
            Path outputPath = new Path(jobOutputDir);
            FileSystem fileSystem = outputPath.getFileSystem(jobClient.getConf());
            FileStatus[] seqFiles = fileSystem.listStatus(outputPath, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return path.getName().endsWith(".seq");
                }
            });


            File downloadDir = new File(localStagingDir, outputPath.getName());
            if (!downloadDir.exists()) {
                downloadDir.mkdirs();
            }
            for (FileStatus seqFile : seqFiles) {
                Path seqProductPath = seqFile.getPath();
                System.out.println("seqProductPath = " + seqProductPath);
                StreamingProductReader reader = new StreamingProductReader(seqProductPath, jobClient.getConf());
                Product product = reader.readProductNodes(null, null);
                String dimapProductName = seqProductPath.getName().replaceFirst(".seq", ".dim");
                System.out.println("dimapProductName = " + dimapProductName);
                File productFile = new File(downloadDir, dimapProductName);
                ProductIO.writeProduct(product, productFile, ProductIO.DEFAULT_FORMAT_NAME, false);
            }
            // todo - zip or tar.gz all output DIMAPs to outputPath.getName() + ".zip" and remove outputPath.getName()
            // return outputPath.getName() + ".zip";
        } catch (Exception e) {
            throw new ProductionException("Error: " + e.getMessage(), e);
        }

    }

}
