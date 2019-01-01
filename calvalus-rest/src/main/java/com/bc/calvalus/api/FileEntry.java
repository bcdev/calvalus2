package com.bc.calvalus.api;

import javax.xml.bind.annotation.XmlRootElement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
@XmlRootElement
public class FileEntry {

    private static final String SYSTEM_PATH = "/calvalus/software/1.0";
    private static final String USER_HOME = "/calvalus/home";
    private static final SimpleDateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd`T`HH:mm:ss");
    static {
        ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private String name;
    private Date modificationDate;
    private long size;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getModificationDate() {
        return modificationDate;
    }

    public void setModificationDate(Date modificationDate) {
        this.modificationDate = modificationDate;
    }

    public void setModifiationDate(String dateString) throws ParseException {
        modificationDate = ISO_DATE_FORMAT.parse(dateString);
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public String toString() { return String.format("%s %s %s", name, size, ISO_DATE_FORMAT.format(modificationDate)); }
}
