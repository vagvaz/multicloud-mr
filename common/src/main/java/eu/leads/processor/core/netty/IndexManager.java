package eu.leads.processor.core.netty;

import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.IntermediateDataIndex;
import eu.leads.processor.core.LevelDBIndex;
import eu.leads.processor.core.MapDBIndex;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This class will handle the puts in each index. Also it will provide the index to Reducers
 * Created by vagvaz on 10/21/15.
 */
public class IndexManager {

  static Map<String, IntermediateDataIndex> indexes;
  static boolean useLevelDB;
  static int parallelism = 4;
  /**
   * initialize the basic Structures
   */
  public static void initialize(Properties properties) {
    parallelism =  LQPConfiguration.getInstance().getConfiguration().getInt("node.engine.parallelism", 4);
    indexes = new HashMap<>();
    if (properties != null) {
      useLevelDB = true;
    } else {
      useLevelDB = false;
    }
  }

  /**
   * add key value to an index
   */
  public static void addToIndex(String name, Object key, Object value) {
    String indexName = name;
    if(name.endsWith(".data")){
      int index = Math.abs(key.hashCode()) % parallelism;
      indexName+= Integer.toString(index);
      IntermediateDataIndex indexI = indexes.get(indexName);
      if (indexI == null) {
        synchronized (indexes) {
          indexI = indexes.get(indexName);
          if (indexI != null) {
            indexI.put(key, value);
            return;
          }
          indexI = initializeIndex(indexName);
          indexes.put(indexName, indexI);
        }
      }
      indexI.put(key, value);
      return;
    }

    IntermediateDataIndex index = indexes.get(indexName);
    if (index == null) {
      synchronized (indexes) {
        index = indexes.get(indexName);
        if (index != null) {
          index.put(key, value);
          return;
        }
        index = initializeIndex(indexName);
        indexes.put(indexName, index);
      }
    }
    index.put(key, value);
  }

  private static IntermediateDataIndex initializeIndex(String indexName) {
    if (useLevelDB) {
      return new LevelDBIndex("/tmp/leadsprocessor-data/leveldbIndex/"+indexName, indexName);
    } else {
      return new MapDBIndex("/tmp/leadsprocessor-data/leveldbIndex/"+indexName, indexName);
    }
  }

  public static IntermediateDataIndex getIndex(String indexName) {
    IntermediateDataIndex index = indexes.get(indexName);
    if (index == null) {
      synchronized (indexes) {
        index = initializeIndex(indexName);
        indexes.put(indexName, index);
      }
    }
    return index;
  }

  public static void removeIndex(String name) {

    if(name.endsWith(".data")){

      for (int i = 0; i < parallelism; i++) {
        IntermediateDataIndex index = indexes.get(name+i);
        if(index != null)
        {
          index.close();
        }
      }
      return;
    }else{
      IntermediateDataIndex index = indexes.get(name);
      if(index != null){
        index.close();
      }

    }
  }
}
