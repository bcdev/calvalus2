package com.bc.calvalus.urban.account;

import java.util.Date;
import lombok.Value;

/**
 * @author muhammad.bc.
 */
@Value
public class Compound {
    private String id;
    private String name;
    private String type;
    private String uri;
    private Date timestamp;
}
