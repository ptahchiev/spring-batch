package org.springframework.batch.item.intf;

/**
 * @author Petar Tahchiev
 * @since 1.0
 */
public interface Entity {

    Long getId();

    void setId(Long id);

    Integer getVersion();

    void setVersion(Integer version);
}
