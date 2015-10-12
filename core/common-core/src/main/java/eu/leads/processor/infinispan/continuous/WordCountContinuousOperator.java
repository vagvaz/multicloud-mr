package eu.leads.processor.infinispan.continuous;

import eu.leads.processor.infinispan.LeadsMapper;
import eu.leads.processor.infinispan.LeadsReducer;
import eu.leads.processor.infinispan.operators.mapreduce.WordCountMapper;
import eu.leads.processor.infinispan.operators.mapreduce.WordCountReducer;

/**
 * Created by vagvaz on 10/6/15.
 */
public class WordCountContinuousOperator extends MapReduceContinuousOperator {
  @Override protected LeadsReducer getReducer() {
    return new WordCountReducer(conf.toString());
  }

  @Override protected LeadsReducer getLocalReducer() {
    return new WordCountReducer(conf.toString());
  }

  @Override protected LeadsMapper getMapper() {
    return new WordCountMapper(conf.toString());
  }

}
