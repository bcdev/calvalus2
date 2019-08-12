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

    private static final SimpleDateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    static {
        ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private String name;
    private String owner;
    private Date modificationDate;
    private long size;

    public FileEntry() {}

    public FileEntry(String name, String owner, Date modificationDate, long size) {
        this.name = name;
        this.owner = owner;
        this.modificationDate = modificationDate;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() { return owner; }

    public void setOwner(String owner) { this.owner = owner; }

    public String getModificationDate() {
        return ISO_DATE_FORMAT.format(modificationDate);
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
    public String toString() { return String.format("%s %s %s %d", name, owner, ISO_DATE_FORMAT.format(modificationDate), size); }
}
