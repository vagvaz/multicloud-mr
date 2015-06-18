package eu.leads.processor.infinispan.operators;

import eu.leads.processor.common.utils.ProfileEvent;
import eu.leads.processor.core.Tuple;
import eu.leads.processor.infinispan.LeadsCollector;
import eu.leads.processor.infinispan.LeadsReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

import java.util.*;

/**
 * Created by vagvaz on 11/21/14.
 */
public class JoinReducer extends LeadsReducer<String,Tuple> {
    //   transient JsonObject conf;
    //   String configString;
    private transient String prefix;
    transient Logger profilerLog;
    protected ProfileEvent profCallable;
    transient ProfileEvent reduceEvent;
    transient Map<String,List<Tuple>> relations;
    transient ArrayList<List<Tuple>> arrays;
    public JoinReducer(String s) {
        super(s);
        configString = s;
        //profilerLog  = LoggerFactory.getLogger("###PROF###" + this.getClass().toString());
        //profCallable = new ProfileEvent("JoinReducer Construct" + this.getClass().toString(),profilerLog);
    }

    @Override
    public void initialize() {
//        profilerLog  = LoggerFactory.getLogger("###PROF###" +  this.getClass().toString());
//        profCallable.setProfileLogger(profilerLog);
//        if(profCallable!=null) {
//            profCallable.end("reduce init ");
//        } else {
//            profCallable = new ProfileEvent("reduce init " + this.getClass().toString(), profilerLog);
//        }
//        profCallable.start("reduce init ");
        super.initialize();
        profilerLog  = LoggerFactory.getLogger("###PROF###" +  this.getClass().toString());
        profCallable = new ProfileEvent("JoinReducer profCallable",profilerLog);
        reduceEvent = new ProfileEvent("JoinReducer ReduceExecute",profilerLog);
        isInitialized = true;
        conf = new JsonObject(configString);
        prefix = outputCacheName+":";
         relations = new HashMap<>();
        arrays = new ArrayList<>(2);

        //      prefix = outputCacheName+":";
        //      outputCache = (Cache) InfinispanClusterSingleton.getInstance().getManager().getPersisentCache(conf.getString("output"));
//        profCallable.end("reduce init");
    }

    @Override
    public void reduce(String reducedKey, Iterator<Tuple> iter,LeadsCollector collector) {


//        profCallable.setProfileLogger(profilerLog);
//        if(profCallable!=null) {
//            profCallable.end("reduce reduce ");
//        } else {
//            profCallable = new ProfileEvent("reduce reduce " + this.getClass().toString(), profilerLog);
//        }

        if(!isInitialized)
            initialize();
        reduceEvent.start("JoinReducer ReduceExecute");
        ProfileEvent tmpprofCallable = new ProfileEvent("JoinReducer Manager " + this.getClass().toString(),
            profilerLog);


        profCallable.start("reduce proc ");
        tmpprofCallable.start("JoinReducerClearPreviousTuples");
        for(Map.Entry<String,List<Tuple>> entry : relations.entrySet()){
            entry.getValue().clear();
        }
        tmpprofCallable.end();

        tmpprofCallable.start("JoinReducerManualCallingProfLog");
        profilerLog.error("Sample log");
        tmpprofCallable.end();
        while(true){
            //         String jsonTuple = iter.next();
            //         Tuple t = new Tuple(jsonTuple);
            try {
                Tuple t = null;


                tmpprofCallable.start("reduce next");
                t = (Tuple) iter.next();
                tmpprofCallable.end("reduce next");
                //            if(c instanceof Tuple )
                //            else{
                //                continue;
                //            }

                tmpprofCallable.start("JoinReducerGetTable");
                String table = t.getAttribute("__table");
                tmpprofCallable.end();
                tmpprofCallable.start("JoinReducerRemoveAttributeTable");
                t.removeAttribute("__table");
                tmpprofCallable.end();
                tmpprofCallable.start("JoinReducerGetRelationArray");
                List<Tuple> tuples = relations.get(table);
                if (tuples == null) {
                    tuples = new ArrayList<>();
                    relations.put(table, tuples);
                }
//                assert (t.hasField("__tupleKey"));
                tmpprofCallable.end();
                tmpprofCallable.start("reduce add");
                tuples.add(t);
                tmpprofCallable.end("reduce add");
            }catch (Exception e){
                tmpprofCallable.start("JoinReducerException");
                if(e instanceof NoSuchElementException){
                    profilerLog.info("End of LeadsIntermediateIterator");
                    tmpprofCallable.end("JoinReducerException IterationEnd");
                    break;
                }
                else{

                    profilerLog.error("EXCEPTION WHILE updating agg value");
                    profilerLog.error(e.getClass() + " " + e.getMessage());
                    profilerLog.error(iter.toString());
                    tmpprofCallable.end("JoinReducerException ExceptionEnd");
                }
            }
        }
        profCallable.end("reduce proc ");

//        profilerLog  = LoggerFactory.getLogger("###PROF###" +  this.getClass().toString());
//        profCallable.setProfileLogger(profilerLog);
        if(profCallable!=null) {
            profCallable.end("reduce reduce ");
        } else {
            profCallable = new ProfileEvent("reduce reduce " + this.getClass().toString(), profilerLog);
        }

        profCallable.start("reduce rest ");
        if(relations.size() < 2)
        {
            profCallable.end("reduce rest ");
            reduceEvent.end();
            return;
        }
//         arrays = new ArrayList<>(2);
        tmpprofCallable.start("JoinReducerToArrays");
        arrays.clear();
        for(List<Tuple> a : relations.values()){
            arrays.add(a);
        }
        tmpprofCallable.end();
        ProfileEvent tmpProfileEvent = new ProfileEvent("tmp profile event",profilerLog);
        try {
            for (int i = 0; i < arrays.get(0).size(); i++) {
                tmpProfileEvent.start("JoinReducerReadOuterTuple");
                Tuple outerTuple = arrays.get(0).get(i);
                //            assert(outerTuple.hasField("__tupleKey"));
                //            System.err.println("outer " + outerTuple.toString());
                String outerKey = outerTuple.getAttribute("__tupleKey");
                if (outerKey == null) {
                    profilerLog.error("outerTuple " + outerTuple.toString());
                }
                tmpProfileEvent.end();
                for (int j = 0; j < arrays.get(1).size(); j++) {
                    tmpProfileEvent.start("JoinReducerReadInnerTuple");
                    Tuple innerTuple = arrays.get(1).get(j);
                    //                outerTuple.removeAttribute("__tupleKey");
                    String outerKey2 = innerTuple.getAttribute("__tupleKey");
                    if (outerKey2 == null) {
                        profilerLog.error("innerTuple " + innerTuple.toString());
                    }
                    tmpProfileEvent.end();
                    tmpProfileEvent.start("JoinReducerJoinTuples");
                    //                assert(innerTuple.hasField("__tupleKey"));
                    Tuple resultTuple = new Tuple(innerTuple, outerTuple, null);
                    //                resultTuple.removeAttribute("__tupleKey");
                    String combinedKey = outerKey + "-" + outerKey2;
                    tmpProfileEvent.end();
                    tmpProfileEvent.start("JoinReducerPrepareOutput");
                    resultTuple = prepareOutput(resultTuple);
                    tmpProfileEvent.end();
                    //                resultTuple = prepareOutput(resultTuple);
                    //            outputCache.put(combinedKey, resultTuple.asJsonObject().toString());
                    tmpProfileEvent.start("JoinReducerEmitResult");
                    collector.emit(prefix + combinedKey, resultTuple);
                    tmpProfileEvent.end();
                }
            }
        }catch(Exception e){
            profCallable.end("JoinReducerRest Exception");
        }
        profCallable.end("reduce rest ");
        reduceEvent.end();
        return ;
    }
}
