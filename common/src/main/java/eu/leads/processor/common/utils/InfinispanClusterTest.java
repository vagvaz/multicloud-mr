package eu.leads.processor.common.utils;

import eu.leads.processor.common.StringConstants;
import eu.leads.processor.common.infinispan.CacheManagerFactory;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.conf.LQPConfiguration;
import eu.leads.processor.core.Tuple;
import org.infinispan.Cache;

import java.io.IOException;
import java.util.Map;

public class InfinispanClusterTest  {
    public static void main(String[] args) throws IOException {
        LQPConfiguration.initialize();
       LQPConfiguration.getInstance().getConfiguration().setProperty("node.current.component", "catalog-worker");
       InfinispanManager manager = InfinispanClusterSingleton.getInstance().getManager();
       Cache map = (Cache) manager.getPersisentCache(StringConstants.QUERIESCACHE);
       Cache map2 = (Cache) manager.getPersisentCache("default.webpages");
//               map2.put("1","11");
//               map2.put("1","11");
//               map.put("1","11");
//               map.put("22", "222");
       LQPConfiguration.getInstance().getConfiguration().setProperty("node.current.component", "planner");
       InfinispanManager manager2 = manager;// CacheManagerFactory.createCacheManager();
      Tuple t = new Tuple();
      t.setAttribute("aaaa","dfdsfadfafd");
      t.setAttribute("aaaeeqa","dfdsfadfafd");
      t.setAttribute("aaawerwa","dfdsfadfafd");
      t.setAttribute("aaaewa","dfdsfadfafd");
      t.setAttribute("aaeaa","dfdsfadfafd");
      t.setAttribute("aeaa2a","dfdsfadfafd");
      t.setAttribute("aeaa432a","dfdsfadfafd");
      t.setAttribute("ae343aaa","dfdsfadfafd");
      t.setAttribute("c",t.asJsonObject().copy().toString());
      t.setAttribute("c2",t.asJsonObject().copy().toString());
      t.setAttribute("c4",t.asJsonObject().copy().toString());

      for(int i = 0; i < 1000000;i++){
        map.put(Integer.toString(i), new Tuple(t.asJsonObject().copy().toString()));
        map2.put(Integer.toString(i), new Tuple(t.asJsonObject().copy().toString()));
        if(i % 50000 == 0 || i < 1024)
        {
          System.out.println("map " + map.getAdvancedCache().getDataContainer().size());
          System.out.println("map2 " + map2.getAdvancedCache().getDataContainer().size());
        }
      }
      System.out.println("Fin ");
      System.in.read();
      System.out.println("map " + map.getAdvancedCache().getDataContainer().size());
      System.out.println("map2 " + map2.getAdvancedCache().getDataContainer().size());
//       map2 = manager2.getPersisentCache("default.webpages");
//       map = manager2.getPersisentCache(StringConstants.QUERIESCACHE);
//
//               map2.put("2333","223");
//               map.put("d2","22");
//               map.put("2f", "22");
//        PrintUtilities.printMap(map);
//        PrintUtilities.printMap(map2);
      System.in.read();
      manager2.stopManager();
      if(manager != manager2) {
        manager.stopManager();
      }

        System.out.println("Restarting....");
        LQPConfiguration.getInstance().getConfiguration().setProperty("node.current.component",
            "catalog-worker");
        manager.startManager("conf/infinispan-clustered.xml");
        LQPConfiguration.getInstance().getConfiguration().setProperty("node.current.component",
            "planner");

        manager2.startManager("conf/infinispan-clustered.xml");
        map2 = (Cache) manager.getPersisentCache("default.webpages");

        PrintUtilities.printMap(map2);
        map2 = (Cache) manager2.getPersisentCache("default.webpages");
        PrintUtilities.printMap(map2);
        manager.stopManager();
        manager2.stopManager();
        //        InfinispanCluster cluster = WeldContext.INSTANCE.getBean(InfinispanCluster.class);
        //
        ////        cluster.initialize();
        //        InfinispanManager man = cluster.getManager();
        //
        //        ConcurrentMap map = man.getPersisentCache("testCache");
        //        map.put("1","11");
        //        map.put("2","22");
        //        InfinispanCluster cluster2 = WeldContext.INSTANCE.getBean(InfinispanCluster.class);
        //
        //        PrintUtilities.printMap(map);
        //        ConcurrentMap map2 = cluster2.getManager().getPersisentCache("testCache");
        //        map2.put("3","33");
        //        PrintUtilities.printMap(map2);
        //        System.out.println("cl");
        //        PrintUtilities.printList(cluster.getManager().getMembers());
        //        System.out.println("cl2");
        //        PrintUtilities.printList(cluster2.getManager().getMembers());
    }

    public void testGetManager() throws Exception {
        //        LQPConfiguration.initialize();
        //        InfinispanCluster cluster = WeldContext.INSTANCE.getBean(InfinispanCluster.class);
        //
        ////        cluster.initialize();
        //        InfinispanManager man = cluster.getManager();
        //
        //        ConcurrentMap map = man.getPersisentCache("testCache");
        //        map.put("1","11");
        //        map.put("2","22");
        //        InfinispanCluster cluster2 = WeldContext.INSTANCE.getBean(InfinispanCluster.class);
        //
        //        PrintUtilities.printMap(map);
        //        ConcurrentMap map2 = cluster2.getManager().getPersisentCache("testCache");
        //        map2.put("3","33");
        //        PrintUtilities.printMap(map2);
        //        System.out.println("cl");
        //        PrintUtilities.printList(cluster.getManager().getMembers());
        //        System.out.println("cl2");
        //        PrintUtilities.printList(cluster2.getManager().getMembers());
    }
}
