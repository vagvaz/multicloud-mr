package eu.leads.processor.infinispan;

import eu.leads.processor.common.infinispan.KeyValueDataTransfer;
import org.infinispan.commons.api.BasicCache;

import java.util.List;
import java.util.Map;

/**
 * Created by vagvaz on 19/12/15.
 */
public class CombineRunnable<KOut,VOut> implements Runnable {
  private final int lastSize;
  private final LocalCollector localCollector;
  private final double percent;
  private  ComplexIntermediateKey baseIntermKey;
  private  int counter;
  private  KeyValueDataTransfer keyValueDataTransfer;
  private  LeadsCombiner combiner;
  private Map<KOut, List<VOut>> buffer;
  private BasicCache intermediateDataCache;

  public <KOut, VOut> CombineRunnable(LeadsCombiner<KOut, VOut> combiner, Map buffer,
      KeyValueDataTransfer keyValueDataTransfer, BasicCache intermediateDataCache,int lastSize,int counter, ComplexIntermediateKey baseIntermKey,LocalCollector localCollector,double percent) {
    this.combiner =  combiner;
    this.buffer = buffer;
    this.keyValueDataTransfer = keyValueDataTransfer;
    this.lastSize = lastSize;
    this.counter = counter;
    this.baseIntermKey = baseIntermKey;
    this.intermediateDataCache = intermediateDataCache;
    this.localCollector = localCollector;
    this.percent = percent;
  }

  @Override public void run() {
    //    inCombine = true;
    //    int lastSize = emitCount;
    //    if(!dontCombine) {
    if (lastSize != buffer.size()) {
      for (Map.Entry<KOut,List<VOut>> entry : buffer.entrySet()) {
        List<VOut> list = entry.getValue();
        if (list.size() > 1) {
          combiner.reduce(entry.getKey(), list.iterator(), localCollector);
        }else{
          localCollector.getCombinedValues().put(entry.getKey(),entry.getValue());
        }

      }
    }
    Map<KOut, List<VOut>> combinedValues = null;
    if(lastSize != buffer.size()){
      combinedValues = localCollector.getCombinedValues();
    } else{
      combinedValues = buffer;
    }
    if(lastSize * percent <= combinedValues.size()) {
      for (Map.Entry<KOut, List<VOut>> entry : combinedValues.entrySet()) {
        output(entry.getKey(), entry.getValue().get(0));
      }
      localCollector.reset();
    }
    buffer.clear();
  }

  private void output(KOut key, VOut value) {
    counter++;

    baseIntermKey.setKey(key.toString());
    baseIntermKey.setCounter(counter);
        ComplexIntermediateKey newKey =
            new ComplexIntermediateKey(baseIntermKey.getSite(), baseIntermKey.getNode(), key.toString(),
                baseIntermKey.getCache(), counter);
    keyValueDataTransfer.putToCache(intermediateDataCache, newKey, value);
  }
}
