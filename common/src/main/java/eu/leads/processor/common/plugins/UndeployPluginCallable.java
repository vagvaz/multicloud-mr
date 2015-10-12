package eu.leads.processor.common.plugins;

import eu.leads.processor.plugins.SimplePluginRunner;
import org.infinispan.Cache;
import org.infinispan.distexec.DistributedCallable;

import java.util.Iterator;
import java.util.Set;

/**
 * Created by vagvaz on 6/6/14.
 */
public class UndeployPluginCallable<K, V> implements DistributedCallable<K, V, Void> {
    private static final long serialVersionUID = 8331682008912636721L;
    private final String cacheName;
    private final String pluginId;
    //    private final Configuration configuration;
    private transient Cache<K, V> cache;

    public UndeployPluginCallable(String name, String pluginId) {
        this.cacheName = name;
        this.pluginId = pluginId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
        cache = cache.getCacheManager().getCache(cacheName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Void call() throws Exception {
        Set<Object> listeners = cache.getListeners();
        Iterator<Object> iterator = listeners.iterator();
        SimplePluginRunner runner = null;
        while (iterator.hasNext()) {
            Object current = iterator.next();
            if (current.getClass().equals(SimplePluginRunner.class)) {
                runner = (SimplePluginRunner) current;
            }
        }
        if (runner == null) {
            System.out.println("On this Cache there is no PluginRunner");
            return null;
        } else {
            runner.undeploy(pluginId);
        }
        return null;
    }
}
