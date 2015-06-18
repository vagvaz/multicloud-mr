package eu.leads.processor.infinispan;

import java.io.Serializable;

public class LeadsReducerCallable<kOut, vOut> extends LeadsBaseCallable<kOut,Object> implements
                                                                              Serializable {

    /**
     * tr
     */
    private static final long serialVersionUID = 3724554288677416505L;
    private LeadsReducer<kOut, vOut> reducer = null;
    private LeadsCollector collector;
    private String prefix;


    public LeadsReducerCallable(String cacheName,
                                 LeadsReducer<kOut, vOut> reducer,String prefix) {
        super("{}",cacheName);
        this.reducer = reducer;
        collector = new LeadsCollector(1000,cacheName);
        collector.setOnMap(false);
        this.prefix = prefix;
    }

  @Override public void executeOn(kOut key, Object value) {
    LeadsIntermediateIterator<vOut> values = new LeadsIntermediateIterator<>((String) key,prefix,imanager);
    reducer.reduce(key,values,collector);
  }

  @Override public void initialize() {
    super.initialize();
      collector.setOnMap(false);
      collector.setEmanager(emanager);

      collector.initializeCache(inputCache.getName(),imanager);


     this.reducer.initialize();
  }
  //    public vOut call() throws Exception {
//        if (reducer == null) {
//            System.out.println(" Reducer not initialized ");
//        } else {
//           reducer.setCacheManager(inputCache.getCacheManager());
//            // System.out.println(" Run Reduce ");
//            vOut result = null;
////            System.out.println("inputCache Cache Size:"
////                    + this.inputCache.size());
////            for (Entry<kOut, List<vOut>> entry : inputCache.entrySet()) {
//          final ClusteringDependentLogic cdl = inputCache.getAdvancedCache().getComponentRegistry().getComponent(ClusteringDependentLogic.class);
//          for(Object ikey : inputCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).keySet()){
//            if(!cdl.localNodeIsPrimaryOwner(ikey))
//              continue;
//                kOut key = (kOut)ikey;
//                List<vOut> list = inputCache.get(key);
//                if(list == null || list.size() == 0){
//                  continue;
//                }
//
//                vOut res = reducer.reduce(key, list.iterator());
//                if(res == null || res.toString().equals("")){
//                  ;
//                }
//                else
//                {
////                  outCache.put(key, res);
//                }
//            }
//
//            return result;
//        }
//        return null;
//    }

  @Override public void finalizeCallable() {
    reducer.finalizeTask();
      super.finalizeCallable();
  }
}
