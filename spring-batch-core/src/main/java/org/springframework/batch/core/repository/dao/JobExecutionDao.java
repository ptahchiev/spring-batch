/*
 * Copyright 2006-2007 the original author or authors.
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

package org.springframework.batch.core.repository.dao;

import org.springframework.batch.core.intf.JobExecution;

import javax.batch.runtime.JobInstance;
import java.util.List;
import java.util.Set;

/**
 * Data Access Object for job executions.
 *
 * @author Lucas Ward
 * @author Robert Kasanicky
 */
public interface JobExecutionDao<JE, I> {

    /**
     * Save a new JobExecution.
     * Preconditions: jobInstance the jobExecution belongs to must have a
     * jobInstanceId.
     *
     * @param jobExecution
     */
    void save(JE jobExecution);

    /**
     * Return all {@link JobExecution} for given {@link JobInstance}, sorted
     * backwards by creation order (so the first element is the most recent).
     */
    List<JE> findAllByJobInstance(I jobInstance);

    /**
     * Find the last {@link JobExecution} to have been created for a given
     * {@link JobInstance}.
     *
     * @param jobInstance the {@link JobInstance}
     * @return the last {@link JobExecution} to execute for this instance
     */
    JE findByJobInstanceOrderByJobExecutionIdAsc(I jobInstance);

    /**
     * @return all {@link JobExecution} that are still running (or indeterminate
     * state), i.e. having null end date, for the specified job name.
     */
    Set<JE> findByJobNameAndEndTimeIsNullOrderByJobExecutionId(String jobName);

    /**
     * @return the {@link JobExecution} for given identifier.
     */
    JE findOne(Long executionId);

    /**
     * Because it may be possible that the status of a JobExecution is updated
     * while running, the following method will synchronize only the status and
     * version fields.
     *
     * @param jobExecution to be updated.
     */
    void synchronizeStatus(JE jobExecution);

    JE getLastJobExecution(I jobInstance);

}
