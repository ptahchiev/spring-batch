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

package org.springframework.batch.core.explore.support;

import java.util.List;
import java.util.Set;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;

/**
 * Implementation of {@link JobExplorer} using the injected DAOs.
 *
 * @author Dave Syer
 * @author Lucas Ward
 * @author Michael Minella
 *
 * @see JobExplorer
 * @see JobInstanceDao
 * @see JobExecutionDao
 * @see StepExecutionDao
 * @since 2.0
 */
public class SimpleJobExplorer implements JobExplorer {

	private JobInstanceDao<JobInstance> jobInstanceDao;

	private JobExecutionDao<JobExecution, JobInstance> jobExecutionDao;

	private StepExecutionDao<StepExecution, JobExecution> stepExecutionDao;

	private ExecutionContextDao<JobExecution, StepExecution> ecDao;

	/**
	 * Provide default constructor with low visibility in case user wants to use
	 * use aop:proxy-target-class="true" for AOP interceptor.
	 */
	SimpleJobExplorer() {
	}

	public SimpleJobExplorer(final JobInstanceDao<JobInstance> jobInstanceDao, final JobExecutionDao<JobExecution, JobInstance> jobExecutionDao,
			final StepExecutionDao<StepExecution, JobExecution> stepExecutionDao, final ExecutionContextDao<JobExecution, StepExecution> ecDao) {
		super();
		this.jobInstanceDao = jobInstanceDao;
		this.jobExecutionDao = jobExecutionDao;
		this.stepExecutionDao = stepExecutionDao;
		this.ecDao = ecDao;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.springframework.batch.core.explore.JobExplorer#findJobExecutions(
	 * org.springframework.batch.core.JobInstance)
	 */
	@Override
	public List<JobExecution> getJobExecutions(final JobInstance jobInstance) {
		final List<JobExecution> executions = jobExecutionDao.findAllByJobInstance(jobInstance);
		for (final JobExecution jobExecution : executions) {
			getJobExecutionDependencies(jobExecution);
			for (final StepExecution stepExecution : jobExecution.getStepExecutions()) {
				getStepExecutionDependencies(stepExecution);
			}
		}
		return executions;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.springframework.batch.core.explore.JobExplorer#findRunningJobExecutions
	 * (java.lang.String)
	 */
	@Override
	public Set<JobExecution> findRunningJobExecutions(final String jobName) {
		final Set<JobExecution> executions = jobExecutionDao.findByJobNameAndEndTimeIsNullOrderByJobExecutionId(jobName);
		for (final JobExecution jobExecution : executions) {
			getJobExecutionDependencies(jobExecution);
			for (final StepExecution stepExecution : jobExecution.getStepExecutions()) {
				getStepExecutionDependencies(stepExecution);
			}
		}
		return executions;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.springframework.batch.core.explore.JobExplorer#getJobExecution(java
	 * .lang.Long)
	 */
	@Override
	public JobExecution getJobExecution(final Long executionId) {
		if (executionId == null) {
			return null;
		}
		final JobExecution jobExecution = jobExecutionDao.findOne(executionId);
		if (jobExecution == null) {
			return null;
		}
		getJobExecutionDependencies(jobExecution);
		for (final StepExecution stepExecution : jobExecution.getStepExecutions()) {
			getStepExecutionDependencies(stepExecution);
		}
		return jobExecution;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.springframework.batch.core.explore.JobExplorer#getStepExecution(java
	 * .lang.Long)
	 */
	@Override
	public StepExecution getStepExecution(final Long jobExecutionId, final Long executionId) {
		final JobExecution jobExecution = jobExecutionDao.findOne(jobExecutionId);
		if (jobExecution == null) {
			return null;
		}
		getJobExecutionDependencies(jobExecution);
		final StepExecution stepExecution = stepExecutionDao.getStepExecution(jobExecution, executionId);
		getStepExecutionDependencies(stepExecution);
		return stepExecution;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.springframework.batch.core.explore.JobExplorer#getJobInstance(java
	 * .lang.Long)
	 */
	@Override
	public JobInstance getJobInstance(final Long instanceId) {
		return jobInstanceDao.getJobInstance(instanceId);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.springframework.batch.core.explore.JobExplorer#getLastJobInstances
	 * (java.lang.String, int)
	 */
	@Override
	public List<JobInstance> getJobInstances(final String jobName, final int start, final int count) {
		return jobInstanceDao.getJobInstances(jobName, start, count);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.springframework.batch.core.explore.JobExplorer#getJobNames()
	 */
	@Override
	public List<String> getJobNames() {
		return jobInstanceDao.getJobNames();
	}

	/* (non-Javadoc)
	 * @see org.springframework.batch.core.explore.JobExplorer#getJobInstanceCount(java.lang.String)
	 */
	@Override
	public int getJobInstanceCount(final String jobName) throws NoSuchJobException {
		return jobInstanceDao.getJobInstanceCount(jobName);
	}

	/*
	 * Find all dependencies for a JobExecution, including JobInstance (which
	 * requires JobParameters) plus StepExecutions
	 */
	private void getJobExecutionDependencies(final JobExecution jobExecution) {

		final JobInstance jobInstance = jobInstanceDao.getJobInstance(jobExecution);
		stepExecutionDao.addStepExecutions(jobExecution);
		jobExecution.setJobInstance(jobInstance);
		jobExecution.setExecutionContext(ecDao.getJobExecutionContext(jobExecution));

	}

	private void getStepExecutionDependencies(final StepExecution stepExecution) {
		if (stepExecution != null) {
			stepExecution.setExecutionContext(ecDao.getStepExecutionContext(stepExecution));
		}
	}
}
