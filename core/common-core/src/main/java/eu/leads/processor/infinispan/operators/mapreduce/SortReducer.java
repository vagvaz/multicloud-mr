package eu.leads.processor.infinispan.operators.mapreduce;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.infinispan.LeadsReducer;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.common.utils.InfinispanUtils;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 12/3/13
 * Time: 10:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class SortReducer extends LeadsReducer<String, Tuple> {
    transient private List<String> sortColumns;
    private List<Boolean> isAscending;
    private List<Boolean> arithmetic;
    private String output;
    ConcurrentMap<String, Tuple> out;

    public SortReducer(JsonObject configuration) {
        super(configuration);
    }

    @Override
    public void initialize() {
        isInitialized = true;
        super.initialize();
        String columns = conf.getString("sortColumns");
        String ascending = conf.getString("ascending");
        String arithm = conf.getString("arithmetic");
        ObjectMapper mapper = new ObjectMapper();
        try {
            sortColumns = mapper.readValue(columns, new TypeReference<List<String>>() {
            });
            isAscending = mapper.readValue(ascending, new TypeReference<List<Boolean>>() {
            });
            arithmetic = mapper.readValue(arithm, new TypeReference<List<Boolean>>() {
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        output = conf.getString("keysName");

    }

    @Override
    public Tuple reduce(String key, Iterator<Tuple> iterator) {
        if (!isInitialized)
            initialize();

        out = InfinispanUtils.getOrCreatePersistentMap(output + key);
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
//        Comparator<Tuple> comparator = new TupleComparator(sortColumns, isAscending, arithmetic);
        while (iterator.hasNext()) {
//            String tmp = iterator.next();
          Tuple tmp = iterator.next();
            tuples.add(tmp);
//            tuples.add(new Tuple(tmp));
            progress();
        }
//        Collections.sort(tuples, comparator);
        int counter = 0;
        for (Tuple t : tuples) {
            out.put(key + ":" + counter, t);
            counter++;
        }
        tuples.clear();
        tuples = null;
//        comparator = null;
//        return output + key;
      return null;
    }
}
