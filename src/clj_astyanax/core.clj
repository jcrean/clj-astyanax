(ns clj-astyanax.core
  (:import
   [java.util UUID]
   [com.netflix.astyanax AstyanaxContext AstyanaxContext$Builder Keyspace MutationBatch]
   [com.netflix.astyanax.model ColumnFamily]
   [com.netflix.astyanax.serializers StringSerializer TimeUUIDSerializer]
   [com.netflix.astyanax.impl AstyanaxConfigurationImpl]
   [com.netflix.astyanax.connectionpool NodeDiscoveryType]
   [com.netflix.astyanax.connectionpool.impl ConnectionPoolConfigurationImpl CountingConnectionPoolMonitor]
   [com.netflix.astyanax.thrift ThriftFamilyFactory]
   [com.netflix.astyanax.util TimeUUIDUtils]))


(def config
     {:cluster            "Test Cluster"
      :keyspace           "jctest"
      :seeds              "127.0.0.1:9160"
      :cql-version        "3.0.0"
      :target-cassandra   "1.2"
      :port               9160
      :max-conns-per-host 1})

(defn build-config [config]
  (.. (AstyanaxConfigurationImpl.)
      (setDiscoveryType NodeDiscoveryType/RING_DESCRIBE)))

(defn build-connection-pool-config [config]
  (.. (ConnectionPoolConfigurationImpl. "MyConfig")
      (setPort (:port config))
      (setMaxConnsPerHost (:max-conns-per-host config))
      (setSeeds (:seeds config))))

(defn build-keyspace [config]
  (.. (AstyanaxContext$Builder.)
      (forCluster (:cluster config))
      (forKeyspace (:keyspace config))
      (withAstyanaxConfiguration (build-config config))
      (withConnectionPoolConfiguration (build-connection-pool-config config))
      (withConnectionPoolMonitor (CountingConnectionPoolMonitor.))
      (buildKeyspace (ThriftFamilyFactory/getInstance))))


(comment
  (def context (build-keyspace "Test Cluster" "test"))

  (.start context)

  (def ks (.getEntity context))

  )


