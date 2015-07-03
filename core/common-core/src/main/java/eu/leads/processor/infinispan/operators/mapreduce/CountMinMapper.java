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
    // TODO(ap0n): Init w, d from config
  }

  @Override
  public void map(String key, Tuple value, Collector<String, Tuple> collector) {
    System.out.println(getClass().getName() + ".map!");
    for (String attribute : value.getFieldNames()) {
      for (String word : value.getAttribute(attribute).split(" ")) {
        // TODO(ap0n): cleaning should go here (if not in the client)
        if (word != null && word.length() > 0) {

          for (int i = 0; i < d; i++) {
            Tuple outputTuple = new Tuple();
            outputTuple.setAttribute("count", 1);
            // emit (<row>,<col>),Tuple
            collector.emit(String.valueOf(i) + "," + String.valueOf(hashRandom(word.hashCode())[d]),
                           outputTuple);
          }
        }
      }
    }
  }

  private synchronized int[] hashRandom(int seed) {
    int[] hash = new int[d];
    random.setSeed(seed);
    for (int i = 0; i < d; i++) {
      hash[i] = random.nextInt(w);
    }
    return hash;
  }
}
