package com.bc.uban.reporting;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author muhammad.bc.
 */
public class ReadRemoteFile {

    private OutputStream outputStream;
    private List<String> recordPerLine = new ArrayList<>();

    public Optional<OutputStream> getOutputStream() {
        return Optional.ofNullable(outputStream);
    }

    public List<String> getRecordPerLine() {
        return recordPerLine;
    }
}
