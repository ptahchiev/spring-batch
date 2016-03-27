package org.springframework.batch.core.intf;

import org.springframework.batch.item.intf.Entity;

import java.util.Date;

/**
 * @author Petar Tahchiev
 * @since 1.0
 */
public interface StepExecution<JE, EC> extends Entity {

    EC getExecutionContext();

    void setExecutionContext(EC executionContext);

    JE getJobExecution();

    String getStepName();

    void setTerminateOnly();

    Date getStartTime();

    Long getJobExecutionId();
}
