package com.bc.calvalus.plot.runtime;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * Parses argument list and collects options "--key=value" or "-key=value" as properties,
 * options without value "--key" or "-key" as properties with value "true",
 * and all other arguments as simple arguments.
 *
 * TODO  move to utility package, or harmonise with existing utilities
 *
 * @author Martin Boettcher
 */
public class Args extends Hashtable<String,String>
{
    private String[] args = null;

    public Args() {}

    public Args(String[] args) {
        load(args);
    }

    /** @return Simple arguments with options stripped off */
    public String[] getArgs() {
        return args;
    }

    /** Parses argument list and collects options and simple arguments */
    public void load(String[] args) {
        if (args == null) return;
        ArrayList<String> accu = new ArrayList<String>(args.length);
        for (String arg : args) {
            if (arg.startsWith("--")) {
                int pos = arg.indexOf('=');
                if (pos != -1) {
                    put(arg.substring(2, pos), arg.substring(pos+1));
                } else {
                    put(arg.substring(2), "true");
                }
            } else if (arg.startsWith("-")) {
                int pos = arg.indexOf('=');
                if (pos != -1) {
                    put(arg.substring(1, pos), arg.substring(pos+1));
                } else {
                    put(arg.substring(1), "true");
                }
            } else {
                 accu.add(arg);
            }
        }
        this.args = accu.toArray(new String[accu.size()]);
    }

    /**
     * Looks up option
     * @param key  name of option
     * @return value of option converted to long
     * @throws IllegalArgumentException if option is not found or cannot be parsed
     */
    public long getLong(String key) throws IllegalArgumentException {
        String value = get(key);
        if (value == null) {
            throw new IllegalArgumentException("missing option " + key);
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("long expected " + " instead of " + value + " for option " + key, ex);
        }
    }

    /**
     * Looks up option with default
     * @param key  name of option
     * @param defaultValue  default value
     * @return value of option converted to long, or default if option is not found
     * @throws IllegalArgumentException if option is set but cannot be parsed
     */
    public long getLong(String key, long defaultValue) throws IllegalArgumentException {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("long expected " + " instead of " + value + " for option " + key, ex);
        }
    }

    /**
     * Looks up option with default
     * @param key  name of option
     * @param defaultValue  default value
     * @return value of option, or default if option is not found
     * */
    public String get(String key, String defaultValue) {  
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }
}
