package com.bc.calvalus.wps.db;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.store.SqlProductionStore;
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
                                  boolean init) throws ProductionException {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new ProductionException("Failed to load database driver " + driver);
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
            throw new ProductionException("Failed to create production store: " + exception.getMessage(), exception);
        }
    }

    public synchronized void addProduction(LocalJob job) {
        cachedJobs.put(job.getId(), job);
        addedJobIds.add(job.getId());
        removedJobIds.remove(job.getId());
    }

    public synchronized void removeProduction(String productionId) {
        cachedJobs.remove(productionId);
        addedJobIds.remove(productionId);
        removedJobIds.add(productionId);
    }

    public synchronized LocalJob[] getProductions() {
        Collection<LocalJob> values = cachedJobs.values();
        return values.toArray(new LocalJob[values.size()]);
    }

    public synchronized LocalJob getProduction(String jobId) {
        return cachedJobs.get(jobId);
    }

    public synchronized void persist() throws ProductionException {
        try {
            for (String productionId : removedJobIds) {
                deleteProduction(productionId);
            }

            for (String productionId : addedJobIds) {
                insertProduction(cachedJobs.get(productionId));
            }

            for (String productionId : cachedJobs.keySet()) {
                if (!addedJobIds.contains(productionId) &&
                    !removedJobIds.contains(productionId)) {
                    updateProductionStatus(cachedJobs.get(productionId));
                }
            }

            connection.commit();

            addedJobIds.clear();
            removedJobIds.clear();

        } catch (SQLException e) {
            throw new ProductionException("Failed to persist production store: " + e.getMessage(), e);
        }
    }

    private void insertProduction(LocalJob job) throws SQLException {
        PreparedStatement insertProductionStmt = connection.prepareStatement("INSERT INTO LocalJob " +
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
        String resultUrlString = constructResultUrlString(status.getResultUrls());
        insertProductionStmt.setString(1, job.getId());
        insertProductionStmt.setString(2, (String) parameters.get("processId"));
        insertProductionStmt.setString(3, (String) parameters.get("productionName"));
        insertProductionStmt.setString(4, (String) parameters.get("productionType"));
        insertProductionStmt.setString(5, (String) parameters.get("geoRegion"));
        insertProductionStmt.setString(6, (String) parameters.get("sourceProduct"));
        insertProductionStmt.setString(7, (String) parameters.get("outputFormat"));
        insertProductionStmt.setString(8, (String) parameters.get("targetDir"));
        insertProductionStmt.setString(9, status.getState());
        insertProductionStmt.setFloat(10, status.getProgress());
        insertProductionStmt.setString(11, status.getMessage());
        insertProductionStmt.setString(12, );
        insertProductionStmt.setString(13, job.getStagingStatus().getState().toString());
        insertProductionStmt.executeUpdate();
    }

    private String constructResultUrlString(List<String> resultUrls) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String resultUrl : resultUrls) {
            stringBuilder.append(resultUrl).append(",");
        }
        String resultUrlString = stringBuilder.toString();
        return resultUrlString.replaceAll(",$", "");
    }

    private void deleteProduction(String productionId) throws SQLException {
        PreparedStatement deleteProductionsStmt = connection.prepareStatement("DELETE FROM production WHERE production_id=?");
        deleteProductionsStmt.setString(1, productionId);
        deleteProductionsStmt.executeUpdate();
    }

    private void init() throws SQLException, IOException {
        try (InputStreamReader streamReader = new InputStreamReader(
                    SqlProductionStore.class.getResourceAsStream("local-store.sql"))) {
            executeAll(connection, streamReader);
            connection.commit();
        }
    }

    private synchronized void update() throws ProductionException {
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
        } catch (SQLException e) {
            throw new ProductionException(e);
        }
    }

    private List<LocalJob> selectJobs() throws SQLException {

        PreparedStatement selectProductions = connection.prepareStatement("SELECT * FROM LocalJob");
        ResultSet resultSet = selectProductions.executeQuery();
        ArrayList<LocalJob> productions = new ArrayList<>(100 + cachedJobs.size());
        while (resultSet.next()) {
            LocalJob localJob = getNextJob(resultSet);
            productions.add(localJob);
        }
        return productions;
    }

    private LocalJob getNextJob(ResultSet resultSet) throws SQLException {
        String jobId = resultSet.getString("job_id");
        String processorId = resultSet.getString("processor_id");
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
        parameters.put("processId", processorId);
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
