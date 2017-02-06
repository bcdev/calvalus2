package com.bc.calvalus.wps.db;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.wps.exceptions.SqlStoreException;
import com.bc.calvalus.wps.localprocess.LocalJob;
import com.bc.calvalus.wps.localprocess.LocalProductionStatus;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author hans
 */
public class SqlStore {

    private final Connection connection;
    private final Map<String, LocalJob> cachedJobs;
    private final Set<String> addedJobIds;
    private final Set<String> removedJobIds;

    private SqlStore(Connection connection) {
        this.connection = connection;
        this.cachedJobs = new HashMap<>();
        this.addedJobIds = new HashSet<>();
        this.removedJobIds = new HashSet<>();
    }

    public static SqlStore create(String driver,
                                  String url,
                                  String user,
                                  String password,
                                  boolean init) throws SqlStoreException {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new SqlStoreException("Failed to load database driver " + driver);
        }

        try {
            Connection connection = DriverManager.getConnection(url, user, password);
            SqlStore store = new SqlStore(connection);
            if (init) {
                store.init();
            } else {
                store.update();
            }
            return store;
        } catch (SQLException | IOException exception) {
            throw new SqlStoreException("Failed to create production store: " + exception.getMessage(), exception);
        }
    }

    public synchronized void addJob(LocalJob job) {
        cachedJobs.put(job.getId(), job);
        addedJobIds.add(job.getId());
        removedJobIds.remove(job.getId());
    }

    public synchronized void updateJob(LocalJob job) {
        if (addedJobIds.contains(job.getId())) {
            cachedJobs.replace(job.getId(), job);
        } else {
            cachedJobs.put(job.getId(), job);
            removedJobIds.remove(job.getId());
        }
    }

    public synchronized void removeJob(String jobId) {
        cachedJobs.remove(jobId);
        addedJobIds.remove(jobId);
        removedJobIds.add(jobId);
    }

    public synchronized LocalJob[] getJobs() {
        Collection<LocalJob> values = cachedJobs.values();
        return values.toArray(new LocalJob[values.size()]);
    }

    public synchronized LocalJob getJob(String jobId) {
        return cachedJobs.get(jobId);
    }

    public synchronized void persist() throws SqlStoreException {
        try {
            for (String jobId : removedJobIds) {
                deleteJob(jobId);
            }

            for (String jobId : addedJobIds) {
                insertJob(cachedJobs.get(jobId));
            }

            for (String jobId : cachedJobs.keySet()) {
                if (!addedJobIds.contains(jobId) &&
                    !removedJobIds.contains(jobId)) {
                    updateJobStatus(cachedJobs.get(jobId));
                }
            }

            connection.commit();

            addedJobIds.clear();
            removedJobIds.clear();

        } catch (SQLException exception) {
            throw new SqlStoreException("Failed to persist production store: " + exception.getMessage(), exception);
        }
    }

    public void close() throws SqlStoreException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new SqlStoreException("Failed to close production store: " + e.getMessage(), e);
        }
    }

    private synchronized void update() throws SqlStoreException {
        try {
            List<LocalJob> jobList = selectJobs();
            for (LocalJob localJob : jobList) {
                LocalJob cachedJob = cachedJobs.get(localJob.getId());
                if (cachedJob != null) {
                    cachedJob.updateStatus(localJob.getStatus());
                } else {
                    cachedJobs.put(localJob.getId(), localJob);
                }
            }
        } catch (SQLException exception) {
            throw new SqlStoreException(exception);
        }
    }

    private void insertJob(LocalJob job) throws SQLException {
        @SuppressWarnings("SqlNoDataSourceInspection")
        PreparedStatement insertJobStmt = connection.prepareStatement("INSERT INTO LocalJob " +
                                                                      "(" +
                                                                      "job_id, " +
                                                                      "process_id, " +
                                                                      "production_name, " +
                                                                      "production_type, " +
                                                                      "geo_region, " +
                                                                      "source_product_name, " +
                                                                      "output_format, " +
                                                                      "target_dir, " +
                                                                      "processing_state, " +
                                                                      "processing_progress, " +
                                                                      "processing_message, " +
                                                                      "result_urls, " +
                                                                      "stop_time" +
                                                                      ") " +
                                                                      " VALUES " +
                                                                      "(" +
                                                                      "?, ?, ?, ?, " +
                                                                      "?, ?, ?, ?, " +
                                                                      "?, ?, ?, ?, " +
                                                                      "?" +
                                                                      ")");
        Map<String, Object> parameters = job.getParameters();
        LocalProductionStatus status = job.getStatus();
        String resultUrlString = "";
        if (status.getResultUrls() != null) {
            resultUrlString = constructResultUrlString(status.getResultUrls());
        }
        insertJobStmt.setString(1, job.getId());
        insertJobStmt.setString(2, (String) parameters.get("processId"));
        insertJobStmt.setString(3, (String) parameters.get("productionName"));
        insertJobStmt.setString(4, (String) parameters.get("productionType"));
        insertJobStmt.setString(5, (String) parameters.get("geoRegion"));
        insertJobStmt.setString(6, (String) parameters.get("sourceProduct"));
        insertJobStmt.setString(7, (String) parameters.get("outputFormat"));
        insertJobStmt.setString(8, (String) parameters.get("targetDir"));
        insertJobStmt.setString(9, status.getState());
        insertJobStmt.setFloat(10, status.getProgress());
        insertJobStmt.setString(11, status.getMessage());
        insertJobStmt.setString(12, resultUrlString);
        insertJobStmt.setTimestamp(13, status.getStopTime() != null ? new Timestamp(status.getStopTime().getTime()) : null);
        insertJobStmt.executeUpdate();
    }

    private void updateJobStatus(LocalJob job) throws SQLException {
        @SuppressWarnings("SqlNoDataSourceInspection")
        PreparedStatement updateJobStmt = connection.prepareStatement("UPDATE LocalJob SET " +
                                                                      "processing_state=?, " +
                                                                      "processing_progress=?, " +
                                                                      "processing_message=?, " +
                                                                      "result_urls=?, " +
                                                                      "stop_time=?" +
                                                                      " WHERE job_id=?");
        LocalProductionStatus status = job.getStatus();
        String resultUrlString = "";
        if (status.getResultUrls() != null) {
            resultUrlString = constructResultUrlString(status.getResultUrls());
        }
        updateJobStmt.setString(1, status.getState());
        updateJobStmt.setFloat(2, status.getProgress());
        updateJobStmt.setString(3, status.getMessage());
        updateJobStmt.setString(4, resultUrlString);
        updateJobStmt.setTimestamp(5, status.getStopTime() != null ? new Timestamp(status.getStopTime().getTime()) : null);
        updateJobStmt.setString(6, job.getId());
        updateJobStmt.executeUpdate();
    }

    private String constructResultUrlString(List<String> resultUrls) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String resultUrl : resultUrls) {
            stringBuilder.append(resultUrl).append(",");
        }
        String resultUrlString = stringBuilder.toString();
        return resultUrlString.replaceAll(",$", "");
    }

    private void deleteJob(String jobId) throws SQLException {
        @SuppressWarnings("SqlNoDataSourceInspection")
        PreparedStatement deleteJobStmt = connection.prepareStatement("DELETE FROM LocalJob WHERE job_id=?");
        deleteJobStmt.setString(1, jobId);
        deleteJobStmt.executeUpdate();
    }

    private void init() throws SQLException, IOException {
        try (InputStreamReader streamReader = new InputStreamReader(SqlStore.class.getResourceAsStream("local-store.sql"))) {
            executeAll(connection, streamReader);
            connection.commit();
        }
    }

    private List<LocalJob> selectJobs() throws SQLException {
        @SuppressWarnings("SqlNoDataSourceInspection")
        PreparedStatement selectJobs = connection.prepareStatement("SELECT * FROM LocalJob");
        ResultSet resultSet = selectJobs.executeQuery();
        ArrayList<LocalJob> jobs = new ArrayList<>(100 + cachedJobs.size());
        while (resultSet.next()) {
            LocalJob localJob = getNextJob(resultSet);
            jobs.add(localJob);
        }
        return jobs;
    }

    private LocalJob getNextJob(ResultSet resultSet) throws SQLException {
        String jobId = resultSet.getString("job_id");
        String processId = resultSet.getString("process_id");
        String productionName = resultSet.getString("production_name");
        String productionType = resultSet.getString("production_type");
        String geoRegion = resultSet.getString("geo_region");
        String sourceProductName = resultSet.getString("source_product_name");
        String outputFormat = resultSet.getString("output_format");
        String targetDir = resultSet.getString("target_dir");
        String processingState = resultSet.getString("processing_state");
        float processingProgress = resultSet.getFloat("processing_progress");
        String processingMessage = resultSet.getString("processing_message");
        String resultUrlString = resultSet.getString("result_urls");
        Timestamp stopTime = resultSet.getTimestamp("stop_time");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("processId", processId);
        parameters.put("productionName", productionName);
        parameters.put("geoRegion", geoRegion);
        parameters.put("outputFormat", outputFormat);
        parameters.put("productionType", productionType);
        parameters.put("sourceProduct", sourceProductName);
        parameters.put("targetDir", targetDir);

        List<String> resultUrls = parseResultUrls(resultUrlString);
        LocalProductionStatus status = new LocalProductionStatus(jobId,
                                                                 ProcessState.valueOf(processingState),
                                                                 processingProgress,
                                                                 processingMessage,
                                                                 resultUrls);
        if (stopTime != null) {
            status.setStopDate(stopTime);
        }
        return new LocalJob(jobId, parameters, status);
    }

    private List<String> parseResultUrls(String resultUrlString) {
        String resultUrlStringArray[] = resultUrlString.split(",");
        return Arrays.asList(resultUrlStringArray);
    }

    private void executeAll(Connection connection, Reader reader) throws SQLException, IOException {
        String sql;
        Statement statement = connection.createStatement();
        LineNumberReader lineNumberReader = new LineNumberReader(reader);
        while ((sql = readSql(lineNumberReader)) != null) {
            statement.executeUpdate(sql);
        }
        statement.close();
    }

    private String readSql(LineNumberReader lineNumberReader) throws IOException {
        StringBuilder sql = null;
        while (true) {
            String line = lineNumberReader.readLine();
            if (line == null) {
                break;
            }
            String sqlPart = line.trim();
            if (!sqlPart.isEmpty() && !sqlPart.startsWith("#")) {
                if (sql == null) {
                    sql = new StringBuilder();
                }
                sql.append(sqlPart);
                if (sqlPart.endsWith(";")) {
                    return sql.substring(0, sql.length() - 1);
                }
                sql.append(" ");
            }
        }
        return sql != null ? sql.toString() : null;
    }

}
