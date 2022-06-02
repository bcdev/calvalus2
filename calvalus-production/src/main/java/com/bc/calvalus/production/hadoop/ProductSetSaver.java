package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.WorkflowStatusEvent;
import com.bc.calvalus.commons.WorkflowStatusListener;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.inventory.ProductSetPersistable;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

class ProductSetSaver implements WorkflowStatusListener {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private final HadoopWorkflowItem productSetWorkflowItem;
    private final ProductSet productSet;
    private final String outputDir;

    public ProductSetSaver(HadoopWorkflowItem productSetWorkflowItem, ProductSet productSet, String outputDir) {
        this.productSetWorkflowItem = productSetWorkflowItem;
        this.productSet = productSet;
        this.outputDir = outputDir;
    }

    ProductSet getProductSet() {
        return productSet;
    }

    @Override
    public void handleStatusChanged(WorkflowStatusEvent event) {
        if (event.getSource() == productSetWorkflowItem && event.getNewStatus().getState() == ProcessState.COMPLETED) {
            String productSetDefinition = ProductSetPersistable.convertToCSV(productSet);
            writeProductSetFile(productSetDefinition);
        }
    }

    private void writeProductSetFile(String text) {
        Path productSetsFile = new Path(outputDir, ProductSetPersistable.FILENAME);
        OutputStreamWriter outputStreamWriter = null;
        try {
            FileSystem fileSystem = productSetWorkflowItem.getProcessingService().getFileSystem(productSetWorkflowItem.getUserName(), productSetsFile.toString());
            OutputStream fsDataOutputStream = fileSystem.create(productSetsFile);
            outputStreamWriter = new OutputStreamWriter(fsDataOutputStream);
            outputStreamWriter.write(text);
        } catch (IOException e) {
            String msg = String.format("Failed to write product set file: %s", e.getMessage());
            LOG.log(Level.SEVERE, msg, e);
        } finally {
            if (outputStreamWriter != null) {
                try {
                    outputStreamWriter.close();
                } catch (IOException ignore) {
                }
            }
        }
    }
}
