/*
 * Copyright 2006-2013 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.batch.runtime.JobInstance;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.intf.JobExecution;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.util.Assert;

/**
 *
 * <p>
 * Implementation of {@link JobRepository} that stores JobInstances,
 * JobExecutions, and StepExecutions using the injected DAOs.
 * <p>
 *
 * @author Lucas Ward
 * @author Dave Syer
 * @author Robert Kasanicky
 * @author David Turanski
 *
 * @see JobRepository
 * @see JobInstanceDao
 * @see JobExecutionDao
 * @see StepExecutionDao
 *
 */
public class SimpleJobRepository<JE extends JobExecution<I, SE>, I extends JobInstance, SE extends StepExecution> implements JobRepository<JE, I, SE> {

	private static final Log logger = LogFactory.getLog(SimpleJobRepository.class);

	private JobInstanceDao<I> jobInstanceDao;

	private JobExecutionDao<JE, I> jobExecutionDao;

	private StepExecutionDao<SE, JE> stepExecutionDao;

	private ExecutionContextDao<JE, SE> ecDao;

	/**
	 * Provide default constructor with low visibility in case user wants to use
	 * use aop:proxy-target-class="true" for AOP interceptor.
	 */
	SimpleJobRepository() {
	}

	public SimpleJobRepository(final JobInstanceDao<I> jobInstanceDao, final JobExecutionDao<JE, I> jobExecutionDao,
			final StepExecutionDao<SE, JE> stepExecutionDao, final ExecutionContextDao<JE, SE> ecDao) {
		super();
		this.jobInstanceDao = jobInstanceDao;
		this.jobExecutionDao = jobExecutionDao;
		this.stepExecutionDao = stepExecutionDao;
		this.ecDao = ecDao;
	}

	@Override
	public boolean isJobInstanceExists(final String jobName, final JobParameters jobParameters) {
		return jobInstanceDao.getJobInstance(jobName, jobParameters) != null;
	}

	@Override
	public JE createJobExecution(final String jobName, final JobParameters jobParameters)
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
		ExecutionContext executionContext;

		// existing job instance found
		if (jobInstance != null) {

			final List<JE> executions = jobExecutionDao.findAllByJobInstance(jobInstance);

			// check for running executions and find the last started
			for (final JobExecution<I, SE> execution : executions) {
				if (execution.isRunning()) {
					throw new JobExecutionAlreadyRunningException("A job execution for this job is already running: "
							+ jobInstance);
				}

				final BatchStatus status = execution.getStatus();
				if (execution.getJobParameters().getParameters().size() > 0 && (status == BatchStatus.COMPLETED || status == BatchStatus.ABANDONED)) {
					throw new JobInstanceAlreadyCompleteException(
							"A job instance already exists and is complete for parameters=" + jobParameters
							+ ".  If you want to run this job again, change the parameters.");
				}
			}
			executionContext = ecDao.getJobExecutionContext(jobExecutionDao.findByJobInstanceOrderByJobExecutionIdAsc(jobInstance));
		}
		else {
			// no job found, create one
			jobInstance = jobInstanceDao.createJobInstance(jobName, jobParameters);
			executionContext = new ExecutionContext();
		}

		final JE jobExecution = (JE) new JobExecution(jobInstance, jobParameters, null);
		jobExecution.setExecutionContext(executionContext);
		jobExecution.setLastUpdated(new Date(System.currentTimeMillis()));

		// Save the JobExecution so that it picks up an ID (useful for clients
		// monitoring asynchronous executions):
		jobExecutionDao.save(jobExecution);
		ecDao.saveJobExecutionContext(jobExecution);

		return jobExecution;

	}

	@Override
	public void update(final JE jobExecution) {

		Assert.notNull(jobExecution, "JobExecution cannot be null.");
		Assert.notNull(jobExecution.getJobId(), "JobExecution must have a Job ID set.");
		Assert.notNull(jobExecution.getId(), "JobExecution must be already saved (have an id assigned).");

		jobExecution.setLastUpdated(new Date(System.currentTimeMillis()));
		jobExecutionDao.save(jobExecution);
	}

	@Override
	public void addStepExecution(final SE stepExecution) {
		validateStepExecution(stepExecution);

		stepExecution.setLastUpdated(new Date(System.currentTimeMillis()));
		stepExecutionDao.saveStepExecution(stepExecution);
		ecDao.saveStepExecutionContext(stepExecution);
	}

	@Override
	public void addAll(final Collection<SE> stepExecutions) {
		Assert.notNull(stepExecutions, "Attempt to save a null collection of step executions");
		for (final SE stepExecution : stepExecutions) {
			validateStepExecution(stepExecution);
			stepExecution.setLastUpdated(new Date(System.currentTimeMillis()));
		}
		stepExecutionDao.saveStepExecutions(stepExecutions);
		ecDao.saveStepExecutionContexts(stepExecutions);
	}

	@Override
	public void updateStepExecution(final SE stepExecution) {
		validateStepExecution(stepExecution);
		Assert.notNull(stepExecution.getId(), "StepExecution must already be saved (have an id assigned)");

		stepExecution.setLastUpdated(new Date(System.currentTimeMillis()));
		stepExecutionDao.updateStepExecution(stepExecution);
		checkForInterruption(stepExecution);
	}

	private void validateStepExecution(final StepExecution stepExecution) {
		Assert.notNull(stepExecution, "StepExecution cannot be null.");
		Assert.notNull(stepExecution.getStepName(), "StepExecution's step name cannot be null.");
		Assert.notNull(stepExecution.getJobExecutionId(), "StepExecution must belong to persisted JobExecution");
	}

	@Override
	public void updateStepExecutionContext(final SE stepExecution) {
		validateStepExecution(stepExecution);
		Assert.notNull(stepExecution.getId(), "StepExecution must already be saved (have an id assigned)");
		ecDao.updateStepExecutionContext(stepExecution);
	}

	@Override
	public void updateJobExecutionContext(final JE jobExecution) {
		ecDao.updateJobExecutionContext(jobExecution);
	}

	@Override
	public StepExecution getLastStepExecution(final I jobInstance, final String stepName) {
		final List<JE> jobExecutions = jobExecutionDao.findAllByJobInstance(jobInstance);
		final List<SE> stepExecutions = new ArrayList<SE>(jobExecutions.size());

		for (final JE jobExecution : jobExecutions) {
			stepExecutionDao.addStepExecutions(jobExecution);
			for (final SE stepExecution : jobExecution.getStepExecutions()) {
				if (stepName.equals(stepExecution.getStepName())) {
					stepExecutions.add(stepExecution);
				}
			}
		}

		SE latest = null;
		for (final SE stepExecution : stepExecutions) {
			if (latest == null) {
				latest = stepExecution;
			}
			if (latest.getStartTime().getTime() < stepExecution.getStartTime().getTime()) {
				latest = stepExecution;
			}
		}

		if (latest != null) {
			final ExecutionContext stepExecutionContext = ecDao.getStepExecutionContext(latest);
			latest.setExecutionContext(stepExecutionContext);
			final ExecutionContext jobExecutionContext = ecDao.getJobExecutionContext(latest.getJobExecution());
			latest.getJobExecution().setExecutionContext(jobExecutionContext);
		}

		return latest;
	}

	/**
	 * @return number of executions of the step within given job instance
	 */
	@Override
	public int getStepExecutionCount(final I jobInstance, final String stepName) {
		int count = 0;
		final List<JE> jobExecutions = jobExecutionDao.findAllByJobInstance(jobInstance);
		for (final JE jobExecution : jobExecutions) {
			stepExecutionDao.addStepExecutions(jobExecution);
			for (final StepExecution stepExecution : jobExecution.getStepExecutions()) {
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
	private void checkForInterruption(final SE stepExecution) {
		final JE jobExecution = stepExecution.getJobExecution();
		jobExecutionDao.synchronizeStatus(jobExecution);
		if (jobExecution.isStopping()) {
			logger.info("Parent JobExecution is stopped, so passing message on to StepExecution");
			stepExecution.setTerminateOnly();
		}
	}

	@Override
	public JE getLastJobExecution(final String jobName, final JobParameters jobParameters) {
		final I jobInstance = jobInstanceDao.getJobInstance(jobName, jobParameters);
		if (jobInstance == null) {
			return null;
		}
		final JE jobExecution = jobExecutionDao.findByJobInstanceOrderByJobExecutionIdAsc(jobInstance);

		if (jobExecution != null) {
			jobExecution.setExecutionContext(ecDao.getJobExecutionContext(jobExecution));
		}
		return jobExecution;

	}

	@Override
	public I createJobInstance(final String jobName, final JobParameters jobParameters) {
		Assert.notNull(jobName, "A job name is required to create a JobInstance");
		Assert.notNull(jobParameters, "Job parameters are required to create a JobInstance");

		final I jobInstance = jobInstanceDao.createJobInstance(jobName, jobParameters);

		return jobInstance;
	}

	@Override
	public JE createJobExecution(final I jobInstance,
			final JobParameters jobParameters, final String jobConfigurationLocation) {

		Assert.notNull(jobInstance, "A JobInstance is required to associate the JobExecution with");
		Assert.notNull(jobParameters, "A JobParameters object is required to create a JobExecution");

		final JE jobExecution = (JE) new org.springframework.batch.core.JobExecution(jobInstance, jobParameters, jobConfigurationLocation);
		final ExecutionContext executionContext = new ExecutionContext();
		jobExecution.setExecutionContext(executionContext);
		jobExecution.setLastUpdated(new Date(System.currentTimeMillis()));

		// Save the JobExecution so that it picks up an ID (useful for clients
		// monitoring asynchronous executions):
		jobExecutionDao.save(jobExecution);
		ecDao.saveJobExecutionContext(jobExecution);

		return jobExecution;
	}
}
