package org.springframework.batch.core.repository.dao;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface JpaJobExecutionDao<T> extends PagingAndSortingRepository<T, Long>, JobExecutionDao, InitializingBean {
}
