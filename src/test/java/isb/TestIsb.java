package isb;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.GlobalStateConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestIsb {

    private final static Logger LOGGER = LoggerFactory.getLogger(TestIsb.class);

    public static final String CACHE_NAME = "application";

    private List<EmbeddedCacheManager> nodes = new ArrayList<>();

    private int nodeSelector = 1;
    private String persistenceLocation;
    private String persistenceLocation2;
    private String persistenceLocation3;

    @Before
    public void createTemporaryLocationName() {
        nodeSelector = 1;
        persistenceLocation = "./target/" + UUID.randomUUID().toString();
        persistenceLocation2 = "./target/" + UUID.randomUUID().toString();
        persistenceLocation3 = "./target/" + UUID.randomUUID().toString();
    }

    private EmbeddedCacheManager startNodes(int owners, String nodeName) {
        LOGGER.info("Start node with owners({})", owners);
        DefaultCacheManager cacheManager = clusteredCacheManager(CacheMode.DIST_SYNC, owners, nodeName);
        nodes.add(cacheManager);
        return cacheManager;
    }

    private void stopNodes() {
        nodes.forEach((e) -> LOGGER.info("Stopping cache manager: [{}]", e.getAddress()));
        nodes.forEach(EmbeddedCacheManager::stop);
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.submit(() -> {
            LOGGER.info("Await for {} nodes shutdown", nodes.size());
            while (nodes.size() > 0) {
                if (nodes.get(0).getStatus().isTerminated()) {
                    nodes.remove(0);
                }
                Thread.sleep(100);
                LOGGER.info("Still waiting for {} nodes shutdown", nodes.size());
            }
            LOGGER.info("Nodes shutdown complete", nodes.size());
            return true;
        });
        try {
            LOGGER.info("Waiting for termination");
            service.shutdown();
            LOGGER.info("Terminated");
            service.awaitTermination(1, TimeUnit.MINUTES);
            LOGGER.info("Shutdown");
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted");
            Thread.currentThread().interrupt();
        }
    }

    private DefaultCacheManager clusteredCacheManager(CacheMode mode, int owners, String nodeName) {
        GlobalStateConfigurationBuilder gscb = new GlobalConfigurationBuilder().clusteredDefault()
                .transport().addProperty("configurationFile", System.getProperty("jgroups.configuration", "jgroups-test-tcp.xml"))
                .clusterName("HACEP")
                .nodeName(nodeName)
                .globalJmxStatistics().allowDuplicateDomains(true).enable()
                .globalState().enable();


        if( nodeSelector == 1 ){
            gscb.persistentLocation(persistenceLocation);
        } else if( nodeSelector == 2) {
            gscb.persistentLocation(persistenceLocation2);
        } else if( nodeSelector == 3) {
            gscb.persistentLocation(persistenceLocation3);
        } else {
            throw new UnsupportedOperationException("Node id unknown");
        }

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.invocationBatching().enable();
        configurationBuilder.clustering().cacheMode(mode).hash().numOwners(owners).groups().enabled();

        org.infinispan.configuration.cache.Configuration loc = extendDefaultConfiguration(configurationBuilder).build();
        return new DefaultCacheManager(gscb.build(), loc, true);
    }

    private ConfigurationBuilder extendDefaultConfiguration(ConfigurationBuilder builder) {

        SingleFileStoreConfigurationBuilder sfcb = builder
                .persistence()
                .passivation(false)
                .addSingleFileStore()
                .shared(false)
                .preload(true)
                .fetchPersistentState(true)
                .purgeOnStartup(false);

        if( nodeSelector == 1 ){
            sfcb.location(persistenceLocation);
        } else if( nodeSelector == 2) {
            sfcb.location(persistenceLocation2);
        } else if( nodeSelector == 3) {
            sfcb.location(persistenceLocation3);
        } else {
            throw new UnsupportedOperationException("Node id unknown");
        }

        sfcb.singleton().enabled(false)
                .eviction()
                .strategy(EvictionStrategy.LRU).type(EvictionType.COUNT).size(1024);

        return builder;
    }

    @Test
    public void testMultiNodePersistence() throws InterruptedException {
        LOGGER.info("Start test serialized rules");

        nodeSelector = 1;
        Cache<String, Object> cache = startNodes(2, "A").getCache(CACHE_NAME);
        nodeSelector = 2;
        Cache<String, Object> cache2 = startNodes(2, "B").getCache(CACHE_NAME);
        nodeSelector = 3;
        Cache<String, Object> cache3 = startNodes(2, "C").getCache(CACHE_NAME);

        String key3 = "3";
        cache3.put(key3, "value3");

        String key2 = "2";
        cache2.put(key2, "value2");

        String key = "1";
        cache.put(key, "value1");

        Object o11 = cache.get(key);
        Object o12 = cache.get(key2);
        Object o13 = cache.get(key3);

        Object o21 = cache2.get(key);
        Object o22 = cache2.get(key2);
        Object o23 = cache2.get(key3);

        Object o31 = cache3.get(key);
        Object o32 = cache3.get(key2);
        Object o33 = cache3.get(key3);

        System.out.println("Expected all not null: ["+o11+" "+o12+" "+o13+"], "+"["+o21+" "+o22+" "+o23+"], "+"["+o31+" "+o32+" "+o33+"]");

        stopNodes();

        nodeSelector = 1;
        Cache<String, Object> cacheDeserialized = startNodes(2, "A").getCache(CACHE_NAME);
        nodeSelector = 2;
        Cache<String, Object> cacheDeserialized2 = startNodes(2, "B").getCache(CACHE_NAME);
        nodeSelector = 3;
        Cache<String, Object> cacheDeserialized3 = startNodes(2, "C").getCache(CACHE_NAME);

        o11 = cacheDeserialized.get(key);
        o12 = cacheDeserialized.get(key2);
        o13 = cacheDeserialized.get(key3);

        o21 = cacheDeserialized2.get(key);
        o22 = cacheDeserialized2.get(key2);
        o23 = cacheDeserialized2.get(key3);

        o31 = cacheDeserialized3.get(key);
        o32 = cacheDeserialized3.get(key2);
        o33 = cacheDeserialized3.get(key3);

        System.out.println("Expected all not null: ["+o11+" "+o12+" "+o13+"], "+"["+o21+" "+o22+" "+o23+"], "+"["+o31+" "+o32+" "+o33+"]");
        Assert.assertNotNull(o11);
        Assert.assertNotNull(o12);
        Assert.assertNotNull(o13);
        Assert.assertNotNull(o21);
        Assert.assertNotNull(o22);
        Assert.assertNotNull(o23);
        Assert.assertNotNull(o31);
        Assert.assertNotNull(o32);
        Assert.assertNotNull(o33);
    }
}

