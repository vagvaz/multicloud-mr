package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.infinispan.operators.mapreduce.WordCountMapper;
import eu.leads.processor.infinispan.operators.mapreduce.WordCountReducer;

import org.vertx.java.core.json.JsonObject;

/**
 * Created by Apostolos Nydriotis on 2015/06/23.
 */
public class WordCountOperator extends MapReduceOperator {

  public WordCountOperator(Node com, InfinispanManager persistence, LogProxy log, Action action) {
    super(com, persistence, log, action);
  }

  @Override
  public void init(JsonObject config) {
    super.init(conf);
    setMapper(new WordCountMapper(conf.toString()));
    setReducer(new WordCountReducer(conf.toString()));
    init_statistics(this.getClass().getCanonicalName());
  }

  @Override
  public void setupMapCallable() {
//      init(conf);
    setMapper(new WordCountMapper(conf.toString()));
    super.setupMapCallable();
  }

  @Override
  public void setupReduceLocalCallable() {

    super.setupReduceLocalCallable();
  }

  @Override
  public void setupReduceCallable() {
    setReducer(new WordCountReducer(conf.toString()));
    super.setupReduceCallable();
  }
}
