package com.bc.calvalus.urban.ws;

import java.net.URI;
import java.time.LocalDateTime;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author muhammad.bc.
 */
@Data
@NoArgsConstructor
public class WpsReport {
    private String jobID;
    private String accRef;
    private String compID;
    private URI uri;
    private LocalDateTime finishDateTime;
}
