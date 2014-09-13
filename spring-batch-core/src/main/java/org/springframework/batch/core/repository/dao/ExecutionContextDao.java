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

package org.springframework.batch.core.repository.dao;

import java.util.Collection;

import org.springframework.batch.item.ExecutionContext;

/**
 * DAO interface for persisting and retrieving {@link ExecutionContext}s.
 * 
 * @author Robert Kasanicky
 * @author David Turanski
 */
public interface ExecutionContextDao<JE, SE, EC> {

	/**
	 * @param jobExecution
	 * @return execution context associated with the given jobExecution
	 */
	EC getJobExecutionContext(JE jobExecution);

	/**
	 * @param stepExecution
	 * @return execution context associated with the given stepExecution
	 */
	EC getStepExecutionContext(SE stepExecution);

	/**
	 * Persist the execution context associated with the given jobExecution,
	 * persistent entry for the context should not exist yet.
	 * @param jobExecution
	 */
	void saveJobExecutionContext(final JE jobExecution);

	/**
	 * Persist the execution context associated with the given stepExecution,
	 * persistent entry for the context should not exist yet.
	 * @param stepExecution
	 */
	void saveStepExecutionContext(final SE stepExecution);

	/**
	 * Persist the execution context associated with each stepExecution in a given collection,
	 * persistent entry for the context should not exist yet.
	 * @param stepExecutions
	 */
	void saveStepExecutionContexts(final Collection<SE> stepExecutions);

	/**
	 * Persist the updates of execution context associated with the given
	 * jobExecution. Persistent entry should already exist for this context.
	 * @param jobExecution
	 */
	void updateJobExecutionContext(final JE jobExecution);

	/**
	 * Persist the updates of execution context associated with the given
	 * stepExecution. Persistent entry should already exist for this context.
	 * @param stepExecution
	 */
	void updateStepExecutionContext(final SE stepExecution);
}
