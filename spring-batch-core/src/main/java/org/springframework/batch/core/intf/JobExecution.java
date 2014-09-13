package org.springframework.batch.core.intf;

import java.util.Collection;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobParameters;

public interface JobExecution<JI, SE, EC> extends Entity {
    
    JI getJobInstance();
    
    Collection<SE> getStepExecutions();
    
    boolean isRunning();
    
    BatchStatus getStatus();
    
    JobParameters getJobParameters();
}
