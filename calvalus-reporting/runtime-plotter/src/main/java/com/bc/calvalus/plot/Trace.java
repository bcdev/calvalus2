package com.bc.calvalus.plot;

import java.util.ArrayList;
import java.util.List;

public class Trace {

    private final String id;
    private long startTime = TimeUtils.TIME_NULL;
    private long stopTime = TimeUtils.TIME_NULL;
    private List<Property> properties = new ArrayList<Property>();

    public Trace(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getStopTime() {
        return stopTime;
    }

    public List<Property> getProperties() {
        return properties;
    }

    public synchronized Property findProperty(String key) {
        for (Property property : properties) {
            if (property.getKey().equals(key)) {
                return property;
            }
        }
        return null;
    }

    public String getPropertyValue(String key) {
        Property property = findProperty(key);
        if (property != null) {
            return property.getValue();
        } else {
            return null;
        }
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setStopTime(long stopTime) {
        this.stopTime = stopTime;
    }

    public synchronized void setProperty(String key, String value) {
        Property property = findProperty(key);
        if (property == null) {
            properties.add(new Property(key, value));
        } else {
            property.setValue(value);
        }
    }

    public boolean isOpen() {
        return stopTime == TimeUtils.TIME_NULL;
    }

    public String toString() {
        StringBuffer accu = new StringBuffer(1024);
        accu.append(id);
        accu.append(' ');
        accu.append(TimeUtils.formatCcsdsUtcFormat(startTime));
        accu.append(' ');
        accu.append(TimeUtils.formatCcsdsUtcFormat(stopTime));
        for (Property property : properties) {
            accu.append(' ');
            accu.append(property.toString());
        }
        return accu.toString();
    }

    static class Property {

        private final String key;
        private String value;

        public Property(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String toString() {
            return key + ":" + value;
        }
    }
}
