package eu.leads.processor.common.infinispan;

import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;

import java.io.Serializable;

/**
 * Created by vagvaz on 9/29/14.
 */
public class AcceptAllFilter implements KeyValueFilter, Serializable {
    @Override public boolean accept(Object key, Object value, Metadata metadata) {
        return true;
    }
}
