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

package org.springframework.batch.core.repository.dao;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.support.transaction.TransactionAwareProxyFactory;
import org.springframework.util.Assert;
import org.springframework.util.SerializationUtils;

/**
 * In-memory implementation of {@link ExecutionContextDao} backed by maps.
 *
 * @author Robert Kasanicky
 * @author Dave Syer
 * @author David Turanski
 */
@SuppressWarnings("serial")
public class MapExecutionContextDao implements ExecutionContextDao<JobExecution, StepExecution, ExecutionContext> {

	private final ConcurrentMap<ContextKey, ExecutionContext> contexts = TransactionAwareProxyFactory
			.createAppendOnlyTransactionalMap();

	private static final class ContextKey implements Comparable<ContextKey>, Serializable {

		private static enum Type { STEP, JOB; }

		private final Type type;
		private final long id;

		private ContextKey(Type type, long id) {
			if(type == null) {
				throw new IllegalStateException("Need a non-null type for a context");
			}
			this.type = type;
			this.id = id;
		}

		@Override
		public int compareTo(ContextKey them) {
			if(them == null) {
				return 1;
			}
			final int idCompare = new Long(this.id).compareTo(new Long(them.id)); // JDK6 Make this Long.compare(x,y)
			if(idCompare != 0) {
				return idCompare;
			}
			final int typeCompare = this.type.compareTo(them.type);
			if(typeCompare != 0) {
				return typeCompare;
			}
			return 0;
		}

		@Override
		public boolean equals(Object them) {
			if(them == null) {
				return false;
			}
			if(them instanceof ContextKey) {
				return this.equals((ContextKey)them);
			}
			return false;
		}

		public boolean equals(final ContextKey them) {
			if(them == null) {
				return false;
			}
			return this.id == them.id && this.type.equals(them.type);
		}

		@Override
		public int hashCode() {
			int value = (int)(id^(id>>>32));
			switch(type) {
			case STEP: return value;
			case JOB: return ~value;
			default: throw new IllegalStateException("Unknown type encountered in switch: " + type);
			}
		}

		public static ContextKey step(long id) { return new ContextKey(Type.STEP, id); }

		public static ContextKey job(long id) { return new ContextKey(Type.JOB, id); }
	}

	public void clear() {
		contexts.clear();
	}

	private static ExecutionContext copy(ExecutionContext original) {
		return (ExecutionContext) SerializationUtils.deserialize(SerializationUtils.serialize(original));
	}

	@Override
	public ExecutionContext getStepExecutionContext(StepExecution stepExecution) {
		return copy(contexts.get(ContextKey.step(stepExecution.getId())));
	}

	@Override
	public void updateStepExecutionContext(StepExecution stepExecution) {
		ExecutionContext executionContext = stepExecution.getExecutionContext();
		if (executionContext != null) {
			contexts.put(ContextKey.step(stepExecution.getId()), copy(executionContext));
		}
	}

	@Override
	public ExecutionContext getJobExecutionContext(JobExecution jobExecution) {
		return copy(contexts.get(ContextKey.job(jobExecution.getId())));
	}

	@Override
	public void updateJobExecutionContext(JobExecution jobExecution) {
		ExecutionContext executionContext = jobExecution.getExecutionContext();
		if (executionContext != null) {
			contexts.put(ContextKey.job(jobExecution.getId()), copy(executionContext));
		}
	}

	@Override
	public void saveJobExecutionContext(JobExecution jobExecution) {
		updateJobExecutionContext(jobExecution);
	}

	@Override
	public void saveStepExecutionContext(StepExecution stepExecution) {
		updateStepExecutionContext(stepExecution);
	}


	@Override
	public void saveStepExecutionContexts(Collection<StepExecution> stepExecutions) {
		Assert.notNull(stepExecutions,"Attempt to save a nulk collection of step executions");
		for (final StepExecution stepExecution: stepExecutions) {
			saveStepExecutionContext(stepExecution);
			saveJobExecutionContext(stepExecution.getJobExecution());
		}
	}

}
