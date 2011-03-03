package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

class HadoopL3ProcessingRequest extends L3ProcessingRequest {
    private final JobClient jobClient;

    HadoopL3ProcessingRequest(JobClient jobClient, ProductionRequest productionRequest) {
        super(productionRequest);
        this.jobClient = jobClient;
    }

    @Override
    public String[] getInputFiles() throws ProductionException {
        Path eoDataRoot = new Path(jobClient.getConf().get("fs.default.name"), "/calvalus/eodata/");
        DateFormat dateFormat = ProductData.UTC.createDateFormat("yyyy-MM-dd");
        Date startDate = null;
        try {
            startDate = dateFormat.parse(getProductionParameterSafe("dateStart"));
        } catch (ParseException ignore) {
            // todo
        }
        Date stopDate = null;
        try {
            stopDate = dateFormat.parse(getProductionParameterSafe("dateStop"));
        } catch (ParseException ignore) {
            // todo
        }
        String inputProductSetId = getProductionParameterSafe("inputProductSetId");
        Path inputPath = new Path(eoDataRoot, inputProductSetId);
        List<String> dateList = getDateList(startDate, stopDate, inputProductSetId);
        try {
            FileSystem fileSystem = inputPath.getFileSystem(jobClient.getConf());
            List<String> inputFileList = new ArrayList<String>();
            for (String day : dateList) {
                FileStatus[] fileStatuses = fileSystem.listStatus(new Path(eoDataRoot, day));
                for (FileStatus fileStatus : fileStatuses) {
                    inputFileList.add(fileStatus.getPath().toString());
                }
            }
            return inputFileList.toArray(new String[inputFileList.size()]);
        } catch (IOException e) {
            throw new ProductionException("Failed to compute input file list.", e);
        }
    }

    static List<String> getDateList(Date start, Date stop, String prefix) {
        Calendar startCal = ProductData.UTC.createCalendar();
        Calendar stopCal = ProductData.UTC.createCalendar();
        startCal.setTime(start);
        stopCal.setTime(stop);
        List<String> list = new ArrayList<String>();
        do {
            String dateString = String.format("MER_RR__1P/r03/%1$tY/%1$tm/%1$td", startCal);
            if (dateString.startsWith(prefix)) {
                list.add(dateString);
            }
            startCal.add(Calendar.DAY_OF_WEEK, 1);
        } while (!startCal.after(stopCal));

        return list;
    }
}
