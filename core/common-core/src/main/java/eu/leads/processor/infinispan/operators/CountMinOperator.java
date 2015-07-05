package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.core.Action;
import eu.leads.processor.core.comp.LogProxy;
import eu.leads.processor.core.net.Node;
import eu.leads.processor.infinispan.operators.mapreduce.CountMinFederationReducer;
import eu.leads.processor.infinispan.operators.mapreduce.CountMinMapper;

import org.vertx.java.core.json.JsonObject;

/**
 * Created by Apostolos Nydriotis on 2015/07/05.
 */
public class CountMinOperator extends MapReduceOperator {

  public CountMinOperator(Node com,
                          InfinispanManager persistence,
                          LogProxy log,
                          Action action) {
    super(com, persistence, log, action);
  }

  @Override
  public void init(JsonObject config) {
    super.init(conf);
    setMapper(new CountMinMapper(conf.toString()));
    setReducer(new CountMinFederationReducer(conf.toString()));
    init_statistics(this.getClass().getCanonicalName());
  }

  @Override
  public void setupMapCallable() {
//      init(conf);
    setMapper(new CountMinMapper(conf.toString()));
    super.setupMapCallable();
  }

  @Override
  public void setupReduceLocalCallable() {
    super.setupReduceLocalCallable();
  }

  @Override
  public void setupReduceCallable() {
    setReducer(new CountMinFederationReducer(conf.toString()));
    super.setupReduceCallable();
  }
}