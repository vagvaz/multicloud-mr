package eu.leads.processor.infinispan;

import eu.leads.processor.core.DataType;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by vagvaz on 20/06/15.
 * This is a class representing a MapReduce Job description
 * MapReduceJob extends DataType a class that contians a json object for storing parameters
 * the JsonObject is named data
 * MapReduceJob attributes:
 * name ( application name wordCount etc )
 * id ( this will be set from the engine and returned to the user for polling when the result is ready
 * configuration ( configuration for the job  )
 * inputs ( name of the input InfinispanCaches )
 * output ( name of the output caches )
 * inputMicroClouds ( which micro clouds to use for running Map and possibly ReduceLocal )
 * outputMicroClouds (which micro clouds to use for output and running the reduce phase )
 * hasReduceLocal
 */
public class MapReduceJob extends DataType {
    public MapReduceJob(JsonObject other) {
        super(other);
    }

    public MapReduceJob(String jsonString) {
        super(jsonString);
    }

    public MapReduceJob() {
        super();
    }

    public String getName() {
        return data.getString("name");
    }

    public void setName(String name) {
        data.putString("name", name);
    }

    public String getId() {
        return data.getString("id");
    }

    public void setId(String id) {
        data.putString("id", id);
    }


    public JsonObject getConfiguration() {
        JsonObject result = data.getObject("configuration");
        return result;
    }

    public void setConfiguration(JsonObject configuration) {
        data.putObject("configuration", configuration);
    }

    public List<String> getInputs() {
        ArrayList<String> result = new ArrayList<String>();
        JsonArray array = data.getArray("inputs");
        if (array != null && array.size() > 0) {
            Iterator<Object> inputIterator = array.iterator();
            while (inputIterator.hasNext()) {
                result.add((String) inputIterator.next());
            }
        }
        return result;
    }

    public void setInputs(List<String> inputs) {
        JsonArray array = new JsonArray();
        for (String input : inputs) {
            array.add(input);
        }
        data.putArray("inputs", array);
    }

    public void addInput(String input) {
        JsonArray array = data.getArray("inputs");
        if (array == null)
            array = new JsonArray();
        array.add(input);
        data.putArray("inputs", array);
    }

    public String getOutput() {
        String result = data.getString("output");
        return result;
    }

    public void setOutput(String output) {
        data.putString("output", output);
    }

    public List<String> getInputMicroCloudss() {
        ArrayList<String> result = new ArrayList<String>();
        JsonArray array = data.getArray("inputMicroClouds");
        if (array != null && array.size() > 0) {
            Iterator<Object> inputIterator = array.iterator();
            while (inputIterator.hasNext()) {
                result.add((String) inputIterator.next());
            }
        }
        return result;
    }

    public void setInputMicroClouds(List<String> microClouds) {
        JsonArray array = new JsonArray();
        for (String input : microClouds) {
            array.add(input);
        }
        data.putArray("inputMicroClouds", array);
    }

    public void addInputMicroCloud(String microCloud) {
        JsonArray array = data.getArray("inputMicroClouds");
        if (array == null)
            array = new JsonArray();
        array.add(microCloud);
        data.putArray("inputMicroClouds", array);
    }

    public ArrayList<String> getOutputMicroClouds() {
        ArrayList<String> result = new ArrayList<String>();
        JsonArray array = data.getArray("outputMicroClouds");
        if (array != null && array.size() > 0) {
            Iterator<Object> inputIterator = array.iterator();
            while (inputIterator.hasNext()) {
                result.add((String) inputIterator.next());
            }
        }
        return result;
    }

    public void setOutputMicroClouds(List<String> microClouds) {
        JsonArray array = new JsonArray();
        for (String input : microClouds) {
            array.add(input);
        }
        data.putArray("outputMicroClouds", array);
    }

    public void addOutputMicroCloud(String microCloud) {
        JsonArray array = data.getArray("outputMicroClouds");
        if (array == null)
            array = new JsonArray();
        array.add(microCloud);
        data.putArray("outputMicroClouds", array);
    }

    public boolean hasReduceLocal() {
        return data.containsField("reduceLocal");
    }

    public void setReduceLocal(boolean runReduceLocal) {
        if (runReduceLocal) {
            data.putString("reduceLocal", "1");
        } else {
            data.removeField("reduceLocal");
        }
    }


}
