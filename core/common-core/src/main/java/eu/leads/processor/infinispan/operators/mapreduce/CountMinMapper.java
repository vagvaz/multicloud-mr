package eu.leads.processor.infinispan.operators.mapreduce;

import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsMapper;

import org.infinispan.distexec.mapreduce.Collector;
import org.vertx.java.core.json.JsonObject;

import java.util.Random;

/**
 * Created by Apostolos Nydriotis on 2015/07/03.
 */
public class CountMinMapper extends LeadsMapper<String, Tuple, String, Tuple> {

  int w, d;
  Random random;

  public CountMinMapper(JsonObject configuration) {
    super(configuration);
    random = new Random();
    // TODO(ap0n): Init w, d from config
  }

  public CountMinMapper(String configuration) {
    super(configuration);
    random = new Random();
    JsonObject operatorConfiguration = new JsonObject(configuration);
//    w = operatorConfiguration.getInteger("w");
//    d = operatorConfiguration.getInteger("d");
  }

  @Override
  public void map(String key, Tuple value, Collector<String, Tuple> collector) {
    for (String attribute : value.getFieldNames()) {
      for (String word : value.getAttribute(attribute).split(" ")) {
        if (word != null && word.length() > 0) {
          for (int i = 0; i < d; i++) {
            // emit <(<row>,<col>), count>
            Tuple output = new Tuple();
            output.setAttribute("count", 1);
            collector.emit(String.valueOf(i) + "," + String.valueOf(hashRandom(word.hashCode())[i]),
                           output);
          }
        }
      }
    }
  }

  @Override
  public void initialize() {
    super.initialize();
    w = conf.getInteger("w");
    d = conf.getInteger("d");
  }

  @Override
  protected void finalizeTask() {
    System.out.println(getClass().getName() + " finished!");
  }

  private int[] hashRandom(int seed) {
    int[] hash = new int[d];
    random.setSeed(seed);
    for (int i = 0; i < d; i++) {
      hash[i] = random.nextInt(w);
    }
    return hash;
  }
}
