package eu.leads.processor.core.netty;

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

  /**
   * initialize the basic Structures
   */
  public static void initialize(Properties properties) {
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
  public static void addToIndex(String indexName, Object key, Object value) {

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
      return new LevelDBIndex("/tmp/leadsprocessor-data/leveldbIndex/", indexName);
    } else {
      return new MapDBIndex("/tmp/leadsprocessor-data/leveldbIndex/", indexName);
    }
  }

  public static IntermediateDataIndex getIndex(String indexName) {
    return indexes.get(indexName);
  }
}
