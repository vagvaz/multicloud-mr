package eu.leads.processor.plugins;

import eu.leads.processor.common.LeadsListener;
import eu.leads.processor.common.infinispan.InfinispanClusterSingleton;
import eu.leads.processor.common.infinispan.InfinispanManager;
import eu.leads.processor.common.utils.FSUtilities;
import eu.leads.processor.conf.ConfigurationUtilities;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.*;

import static eu.leads.processor.plugins.EventType.*;

/**
 * Created by vagvaz on 6/3/14. This is a the listener that is able to run plugins it is an
 * infinsipan listeners that registeres to create,modify,remove events is asynchronous and NOT
 * clustered.
 */
@Listener(clustered = false, sync = false, primaryOnly = true)
public class
        SimplePluginRunner implements LeadsListener {

    transient protected Configuration configuration;
    transient protected Set<PluginInterface> deployedPlugins;
    transient protected HashMap<EventType, List<PluginInterface>> plugins;
    transient protected InfinispanManager manager;
    protected Properties properties;
    protected String id;
    protected String targetCacheName;
    protected String configCacheName;
     protected List<String> pluginNames;
    transient private Logger log = LoggerFactory.getLogger(this.getClass());


    public SimplePluginRunner(String id, Properties properties) {
        this.targetCacheName = properties.getProperty("target");
        this.configCacheName = properties.getProperty("config");
        this.pluginNames = (List<String>) properties.get("pluginNames");
        this.id = id;
    }

    @CacheEntryModified
    public void entryModified(CacheEntryModifiedEvent<Object, Object> event) {
        if (event.isPre())
            return;
        if (event.isCreated())
            return;
        if (event.isCommandRetried())
            return;
        List<PluginInterface> mplugins = plugins.get(MODIFIED);
        for (PluginInterface plugin : mplugins) {
            plugin.modified(event.getKey(), event.getValue(), event.getCache());
        }

    }

    @CacheEntryCreated
    public void entryCreated(CacheEntryCreatedEvent<Object, Object> event) {
        if (event.isPre())
            return;
        if (event.isCommandRetried())
            return;
        List<PluginInterface> cplugins = plugins.get(CREATED);
        for (PluginInterface plugin : cplugins) {
            plugin.created(event.getKey(), event.getValue(), event.getCache());
        }

    }

    @CacheEntryRemoved
    public void entryRemoved(CacheEntryRemovedEvent<Object, Object> event) {
        if (event.isCommandRetried())
            return;
        if (event.isPre())
            return;
        List<PluginInterface> rplugins = plugins.get(REMOVED);
        for (PluginInterface plugin : rplugins) {
            plugin.removed(event.getKey(), event.getValue(), event.getCache());
        }
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        out.defaultWriteObject();
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        //        Cache cache = (Cache) InfinispanClusterSingleton.getInstance().getManager().getPersisentCache(configCacheName);
    }

    public void undeploy(String pluginId) {
        removePluginFrom(pluginId, plugins.get(MODIFIED));
        removePluginFrom(pluginId, plugins.get(CREATED));
        removePluginFrom(pluginId, plugins.get(REMOVED));
    }

    private void removePluginFrom(String pluginId, List<PluginInterface> pluginInterfaces) {
        Iterator<PluginInterface> iterator = pluginInterfaces.iterator();
        while (iterator.hasNext()) {
            PluginInterface plugin = iterator.next();
            if (pluginId.equals(plugin.getId())) {
                pluginInterfaces.remove(plugin);
                return;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InfinispanManager getManager() {
        return this.manager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setManager(InfinispanManager manager) {
        this.manager = manager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(InfinispanManager manager) {
        Cache cache = (Cache) manager.getPersisentCache(configCacheName);
        this.manager = manager;
        for (Object listener : cache.getListeners()) {
            if (listener.getClass().equals(SimplePluginRunner.class)) {
                SimplePluginRunner runner = (SimplePluginRunner) listener;
                if (runner.id.equals(this.id)) {
                    log.info("There is already a SimplePluginRunner running on "
                            + InfinispanClusterSingleton.getInstance().getManager()
                            .getMemberName().toString() + " for  cache "
                            + targetCacheName);
                }

            }
        }
        initializeTransient();
        for (String plugin : pluginNames) {
            initializePlugin(cache, plugin);
        }
    }

    private void initializeTransient() {
        log = LoggerFactory.getLogger(this.getClass());
        configuration = new XMLConfiguration();
        deployedPlugins = new HashSet<PluginInterface>();
        plugins = new HashMap<EventType, List<PluginInterface>>(3);
        plugins.put(EventType.CREATED, new LinkedList<PluginInterface>());
        plugins.put(EventType.MODIFIED, new LinkedList<PluginInterface>());
        plugins.put(EventType.REMOVED, new LinkedList<PluginInterface>());
    }

    private void initializePlugin(Cache cache, String plugin) {
        String jarFileName = null;
        if (plugin.equals("eu.leads.processor.plugins.pagerank.PagerankPlugin")) {
//            ConfigurationUtilities
//                    .addToClassPath(System.getProperty("java.io.tmpdir") + "/leads/plugins/" + "pagerank-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar");
            jarFileName = System.getProperty("java.io.tmpdir") + "/leads/plugins/" + "pagerank-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar";
        } else if (plugin.equals("eu.leads.processor.plugins.sentiment.SentimentAnalysisPlugin")) {
//            ConfigurationUtilities
//                    .addToClassPath(System.getProperty("java.io.tmpdir") + "/leads/plugins/" + "sentiment-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar");
            jarFileName = System.getProperty("java.io.tmpdir") + "/leads/plugins/" + "sentiment-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar";
        } else {
            byte[] jarAsBytes = (byte[]) cache.get(plugin + ":jar");
            FSUtilities.flushPluginToDisk(plugin + ".jar", jarAsBytes);

//            ConfigurationUtilities
//                    .addToClassPath(System.getProperty("java.io.tmpdir") + "/leads/plugins/" + plugin
//                            + ".jar");
            jarFileName = System.getProperty("java.io.tmpdir") + "/leads/plugins/" + plugin
                    + ".jar";
        }
        ClassLoader classLoader = null;
        try {
            classLoader = ConfigurationUtilities.getClassLoaderFor(jarFileName);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        byte[] config = (byte[]) cache.get(plugin + ":conf");
        FSUtilities.flushToTmpDisk("/leads/tmp/" + plugin + "-conf.xml", config);
        XMLConfiguration pluginConfig = null;
        try {
            pluginConfig =
                    new XMLConfiguration(System.getProperty("java.io.tmpdir") + "/leads/tmp/" + plugin
                            + "-conf.xml");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        String className = (String) cache.get(plugin + ":className");
        if (className != null && !className.equals("")) {
            try {
                Class<?> plugClass =
                        Class.forName(className, true, classLoader);
                Constructor<?> con = plugClass.getConstructor();
                PluginInterface plug = (PluginInterface) con.newInstance();
                plug.initialize(pluginConfig, manager);
                Integer events = (Integer) cache.get(plugin + ":events");
                addToEvents(plug, events);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        } else {
            log.error("Could not find the name for " + plugin);
        }

    }

    private void addToEvents(PluginInterface plug, Integer events) {
        switch (events) {
            case 0: //bad
                log.error("No events registered for " + plug.getId());
                break;
            case 1:
                plugins.get(CREATED).add(plug);
                break;
            case 2:
                plugins.get(MODIFIED).add(plug);
                break;
            case 3:
                plugins.get(CREATED).add(plug);
                plugins.get(MODIFIED).add(plug);
                break;
            case 4:
                plugins.get(REMOVED).add(plug);
                break;
            case 5:
                plugins.get(CREATED).add(plug);
                plugins.get(REMOVED).add(plug);
                break;
            case 6:
                plugins.get(MODIFIED).add(plug);
                plugins.get(REMOVED).add(plug);
                break;
            case 7:
                plugins.get(MODIFIED).add(plug);
                plugins.get(REMOVED).add(plug);
                plugins.get(CREATED).add(plug);
                break;
            default:
                log.error("Wrong event number combination " + events);
                break;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return SimplePluginRunner.class.getCanonicalName();
    }

    public void addPlugin(PluginInterface plugin, XMLConfiguration config, int eventmask) {
        plugin.initialize(config, manager);
        addToEvents(plugin, eventmask);
    }
}
