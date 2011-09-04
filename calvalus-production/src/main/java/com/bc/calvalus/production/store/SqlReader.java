package com.bc.calvalus.production.store;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
class SqlReader {

    private final LineNumberReader reader;

    public SqlReader(Reader reader) {
        this.reader = new LineNumberReader(reader);
    }

    public void executeAll(Connection connection) throws SQLException, IOException {
        String sql;
        Statement statement = connection.createStatement();
        while ((sql = readSql()) != null) {
            statement.executeUpdate(sql);
        }
        statement.close();
        connection.commit();
    }


    public String readSql() throws IOException {
        StringBuilder sql = null;
        while (true) {
            String line = reader.readLine();
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
