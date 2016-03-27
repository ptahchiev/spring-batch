package org.springframework.batch.core.intf;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.item.intf.Entity;

import java.util.Collection;

public interface JobExecution<JI, SE, EC> extends Entity {

    Long getJobId();

    JI getJobInstance();

    void setJobInstance(JI jobInstance);

    Collection<SE> getStepExecutions();

    boolean isRunning();

    boolean isStopping();

    BatchStatus getStatus();

    JobParameters getJobParameters();

    EC getExecutionContext();

    void setExecutionContext(EC executionContext);
}
