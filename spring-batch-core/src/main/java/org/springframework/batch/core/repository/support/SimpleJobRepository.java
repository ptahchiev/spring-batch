/*
 * Copyright 2006-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.core.repository.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.intf.JobExecution;
import org.springframework.batch.core.intf.StepExecution;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.item.intf.ExecutionContext;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * * Implementation of {@link JobRepository} that stores JobInstances,
 * JobExecutions, and StepExecutions using the injected DAOs.
 *
 * @author Lucas Ward
 * @author Dave Syer
 * @author Robert Kasanicky
 * @author David Turanski
 * @see JobRepository
 * @see JobInstanceDao
 * @see JobExecutionDao
 * @see StepExecutionDao
 */
public class SimpleJobRepository<JE extends JobExecution<I, SE, EC>, I extends JobInstance, SE extends StepExecution<JE, EC>, EC extends ExecutionContext>
                implements JobRepository<JE, I, SE> {

    private static final Log logger = LogFactory.getLog(SimpleJobRepository.class);

    private JobInstanceDao<I> jobInstanceDao;

    private JobExecutionDao<JE, I> jobExecutionDao;

    private StepExecutionDao<SE, JE> stepExecutionDao;

    private ExecutionContextDao<JE, SE, EC> executionContextDao;

    /**
     * Provide default constructor with low visibility in case user wants to use
     * use aop:proxy-target-class="true" for AOP interceptor.
     */
    SimpleJobRepository() {
    }

    public SimpleJobRepository(JobInstanceDao jobInstanceDao, JobExecutionDao jobExecutionDao, StepExecutionDao stepExecutionDao,
                               ExecutionContextDao executionContextDao) {
        super();
        this.jobInstanceDao = jobInstanceDao;
        this.jobExecutionDao = jobExecutionDao;
        this.stepExecutionDao = stepExecutionDao;
        this.executionContextDao = executionContextDao;
    }

    @Override
    public boolean isJobInstanceExists(String jobName, JobParameters jobParameters) {
        return jobInstanceDao.getJobInstance(jobName, jobParameters) != null;
    }

    @Override
    public JE createJobExecution(String jobName, JobParameters jobParameters)
                    throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");

		/*
         * Find all jobs matching the runtime information.
		 *
		 * If this method is transactional, and the isolation level is
		 * REPEATABLE_READ or better, another launcher trying to start the same
		 * job in another thread or process will block until this transaction
		 * has finished.
		 */

        I jobInstance = jobInstanceDao.getJobInstance(jobName, jobParameters);
        EC executionContext;

        // existing job instance found
        if (jobInstance != null) {

            List<JE> executions = jobExecutionDao.findAllByJobInstance(jobInstance);

            // check for running executions and find the last started

            for (JE execution : executions) {
                if (execution.isRunning() || execution.isStopping()) {
                    throw new JobExecutionAlreadyRunningException("A job execution for this job is already running: " + jobInstance);
                }
                BatchStatus status = execution.getStatus();
                if (status == BatchStatus.UNKNOWN) {
                    throw new JobRestartException(
                                    "Cannot restart job from UNKNOWN status. " + "The last execution ended with a failure that could not be rolled back, "
                                                    + "so it may be dangerous to proceed. Manual intervention is probably necessary.");
                }
                if (execution.getJobParameters().getParameters().size() > 0 && (status == BatchStatus.COMPLETED || status == BatchStatus.ABANDONED)) {
                    throw new JobInstanceAlreadyCompleteException("A job instance already exists and is complete for parameters=" + jobParameters
                                                                                  + ".  If you want to run this job again, change the parameters.");
                }
            }
            executionContext = executionContextDao.getJobExecutionContext(jobExecutionDao.getLastJobExecution(jobInstance));
        } else {
            // no job found, create one
            jobInstance = jobInstanceDao.createJobInstance(jobName, jobParameters);
            executionContext = new org.springframework.batch.item.ExecutionContext();
        }

        JE jobExecution = new org.springframework.batch.core.JobExecution(jobInstance, jobParameters, null);
        jobExecution.setExecutionContext(executionContext);
        jobExecution.setLastModifiedDate(new Date(System.currentTimeMillis()));

        // Save the JobExecution so that it picks up an ID (useful for clients
        // monitoring asynchronous executions):
        jobExecutionDao.save(jobExecution);
        executionContextDao.saveJobExecutionContext(jobExecution);

        return jobExecution;

    }

    @Override
    public void updateJobExecution(JE jobExecution) {

        Assert.notNull(jobExecution, "JobExecution cannot be null.");
        Assert.notNull(jobExecution.getJobId(), "JobExecution must have a Job ID set.");
        Assert.notNull(jobExecution.getId(), "JobExecution must be already saved (have an id assigned).");

        jobExecution.setLastModifiedDate(new Date(System.currentTimeMillis()));

        jobExecutionDao.synchronizeStatus(jobExecution);
        jobExecutionDao.save(jobExecution);
    }

    @Override
    public void add(SE stepExecution) {
        validateStepExecution(stepExecution);

        stepExecution.setLastModifiedDate(new Date(System.currentTimeMillis()));
        stepExecutionDao.saveStepExecution(stepExecution);
        executionContextDao.saveStepExecutionContext(stepExecution);
    }

    @Override
    public void addAll(Collection<SE> stepExecutions) {
        Assert.notNull(stepExecutions, "Attempt to save a null collection of step executions");
        for (SE stepExecution : stepExecutions) {
            validateStepExecution(stepExecution);
            stepExecution.setLastModifiedDate(new Date(System.currentTimeMillis()));
        }
        stepExecutionDao.saveStepExecutions(stepExecutions);
        executionContextDao.saveStepExecutionContexts(stepExecutions);
    }

    @Override
    public void updateStepExecution(SE stepExecution) {
        validateStepExecution(stepExecution);
        Assert.notNull(stepExecution.getId(), "StepExecution must already be saved (have an id assigned)");

        stepExecution.setLastModifiedDate(new Date(System.currentTimeMillis()));
        stepExecutionDao.updateStepExecution(stepExecution);
        checkForInterruption(stepExecution);
    }

    private void validateStepExecution(SE stepExecution) {
        Assert.notNull(stepExecution, "StepExecution cannot be null.");
        Assert.notNull(stepExecution.getStepName(), "StepExecution's step name cannot be null.");
        Assert.notNull(stepExecution.getJobExecutionId(), "StepExecution must belong to persisted JobExecution");
    }

    @Override
    public void updateStepExecutionContext(SE stepExecution) {
        validateStepExecution(stepExecution);
        Assert.notNull(stepExecution.getId(), "StepExecution must already be saved (have an id assigned)");
        executionContextDao.updateStepExecutionContext(stepExecution);
    }

    @Override
    public void updateJobExecutionContext(JE jobExecution) {
        executionContextDao.updateJobExecutionContext(jobExecution);
    }

    @Override
    public SE getLastStepExecution(I jobInstance, String stepName) {
        List<JE> jobExecutions = jobExecutionDao.findAllByJobInstance(jobInstance);
        List<SE> stepExecutions = new ArrayList<SE>(jobExecutions.size());

        for (JE jobExecution : jobExecutions) {
            stepExecutionDao.addStepExecutions(jobExecution);
            for (SE stepExecution : jobExecution.getStepExecutions()) {
                if (stepName.equals(stepExecution.getStepName())) {
                    stepExecutions.add(stepExecution);
                }
            }
        }

        SE latest = null;
        for (SE stepExecution : stepExecutions) {
            if (latest == null) {
                latest = stepExecution;
            }
            if (latest.getStartTime().getTime() < stepExecution.getStartTime().getTime()) {
                latest = stepExecution;
            }
        }

        if (latest != null) {
            EC stepExecutionContext = executionContextDao.getStepExecutionContext(latest);
            latest.setExecutionContext(stepExecutionContext);
            EC jobExecutionContext = executionContextDao.getJobExecutionContext(latest.getJobExecution());
            latest.getJobExecution().setExecutionContext(jobExecutionContext);
        }

        return latest;
    }

    /**
     * @return number of executions of the step within given job instance
     */
    @Override
    public int getStepExecutionCount(I jobInstance, String stepName) {
        int count = 0;
        List<JE> jobExecutions = jobExecutionDao.findAllByJobInstance(jobInstance);
        for (JE jobExecution : jobExecutions) {
            stepExecutionDao.addStepExecutions(jobExecution);
            for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                if (stepName.equals(stepExecution.getStepName())) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Check to determine whether or not the JobExecution that is the parent of
     * the provided StepExecution has been interrupted. If, after synchronizing
     * the status with the database, the status has been updated to STOPPING,
     * then the job has been interrupted.
     *
     * @param stepExecution
     */
    private void checkForInterruption(SE stepExecution) {
        JE jobExecution = stepExecution.getJobExecution();
        jobExecutionDao.synchronizeStatus(jobExecution);
        if (jobExecution.isStopping()) {
            logger.info("Parent JobExecution is stopped, so passing message on to StepExecution");
            stepExecution.setTerminateOnly();
        }
    }

    @Override
    public JE getLastJobExecution(String jobName, JobParameters jobParameters) {
        JobInstance jobInstance = jobInstanceDao.getJobInstance(jobName, jobParameters);
        if (jobInstance == null) {
            return null;
        }
        JE jobExecution = jobExecutionDao.getLastJobExecution(jobInstance);

        if (jobExecution != null) {
            jobExecution.setExecutionContext(executionContextDao.getJobExecutionContext(jobExecution));
            stepExecutionDao.addStepExecutions(jobExecution);
        }
        return jobExecution;

    }

    @Override
    public I createJobInstance(String jobName, JobParameters jobParameters) {
        Assert.notNull(jobName, "A job name is required to create a JobInstance");
        Assert.notNull(jobParameters, "Job parameters are required to create a JobInstance");

        return jobInstanceDao.createJobInstance(jobName, jobParameters);
    }

    @Override
    public JE createJobExecution(I jobInstance, JobParameters jobParameters, String jobConfigurationLocation) {

        Assert.notNull(jobInstance, "A JobInstance is required to associate the JobExecution with");
        Assert.notNull(jobParameters, "A JobParameters object is required to create a JobExecution");

        JE jobExecution = new org.springframework.batch.core.JobExecution(jobInstance, jobParameters, jobConfigurationLocation);
        EC executionContext = new org.springframework.batch.item.ExecutionContext();
        jobExecution.setExecutionContext(executionContext);
        jobExecution.setLastModifiedDate(new Date(System.currentTimeMillis()));

        // Save the JobExecution so that it picks up an ID (useful for clients
        // monitoring asynchronous executions):
        jobExecutionDao.save(jobExecution);
        executionContextDao.saveJobExecutionContext(jobExecution);

        return jobExecution;
    }
}
