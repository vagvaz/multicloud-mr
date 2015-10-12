package eu.leads.processor.infinispan.continuous;

import eu.leads.processor.infinispan.LeadsBaseCallable;
import eu.leads.processor.infinispan.LeadsMapper;
import eu.leads.processor.infinispan.LeadsReducer;
import eu.leads.processor.infinispan.operators.mapreduce.CountMinFederationReducer;
import eu.leads.processor.infinispan.operators.mapreduce.CountMinLocalReducer;
import eu.leads.processor.infinispan.operators.mapreduce.CountMinMapper;

/**
 * Created by vagvaz on 10/12/15.
 */
public class CountMinOperatorContinuous extends MapReduceContinuousOperator {
  @Override protected LeadsReducer getReducer() {
    return new CountMinFederationReducer(conf.toString());
  }

  @Override protected LeadsReducer getLocalReducer() {
    return new CountMinLocalReducer(conf.toString());
  }

  @Override protected LeadsMapper getMapper() {
    return new CountMinMapper(conf.toString());
  }
}
