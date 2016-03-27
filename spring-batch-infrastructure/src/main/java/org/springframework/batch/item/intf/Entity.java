package org.springframework.batch.item.intf;

import java.util.Date;

/**
 * @author Petar Tahchiev
 * @since 1.0
 */
public interface Entity {

    Long getId();

    void setId(Long id);

    Integer getVersion();

    void setVersion(Integer version);

    /* auditable */

    Date getCreatedDate();

    void setCreatedDate(final Date creationDate);

    Date getLastModifiedDate();

    void setLastModifiedDate(final Date lastModifiedDate);
}
