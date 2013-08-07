(ns clj-astyanax.core
  (:use
   clj-astyanax.serializers
   clj-astyanax.ddl.helpers
   clj-astyanax.bindings)
  (:import
   [com.netflix.astyanax.model ColumnFamily]
   [com.netflix.astyanax.thrift ThriftFamilyFactory]
   [com.netflix.astyanax.connectionpool NodeDiscoveryType]
   [com.netflix.astyanax.impl AstyanaxConfigurationImpl]
   [com.netflix.astyanax.connectionpool.impl ConnectionPoolConfigurationImpl CountingConnectionPoolMonitor]
   [com.netflix.astyanax AstyanaxContext AstyanaxContext$Builder Keyspace]))

(defonce cluster-registry (atom {}))

(defn register-cluster [cluster-name config]
  (swap! cluster-registry assoc (keyword cluster-name) config))

(defn lookup-cluster [cluster-name]
  (get @cluster-registry (keyword cluster-name)))

(defn with-cluster* [cluster-name body-fn]
  (let [cfg (get @cluster-registry (keyword cluster-name))]
    (when-not cfg
      (throw (RuntimeException. (format "No cluster registered for %s, currently registered clusters are: [%s]" cluster-name (comma-join (keys @cluster-registry))))))
    (binding [cluster-config cfg]
      (body-fn))))

(defmacro with-cluster [cluster-name & body]
  `(with-cluster* ~cluster-name (fn [] ~@body)))

(defonce keyspace-registry (atom {}))

(defn build-config [config]
  (.. (AstyanaxConfigurationImpl.)
      (setDiscoveryType NodeDiscoveryType/RING_DESCRIBE)
      (setCqlVersion (:cql-version config))
      (setTargetCassandraVersion (:target-cassandra config))))

(defn build-connection-pool-config [config]
  (.. (ConnectionPoolConfigurationImpl. "MyConfig")
      (setPort (:port config))
      (setMaxConnsPerHost (:max-conns-per-host config))
      (setSeeds (:seeds config))))

(defn build-keyspace-ctx [ks-name config]
  (.. (AstyanaxContext$Builder.)
      (forCluster (:cluster config))
      (forKeyspace (safe-name ks-name))
      (withAstyanaxConfiguration (build-config config))
      (withConnectionPoolConfiguration (build-connection-pool-config config))
      (withConnectionPoolMonitor (CountingConnectionPoolMonitor.))
      (buildKeyspace (ThriftFamilyFactory/getInstance))))


(defn unregister-keyspace [ks-name]
  (when-let [ctx (get-in @keyspace-registry [(keyword ks-name) :context])]
    (.shutdown ctx))
  (swap! keyspace-registry dissoc (keyword ks-name)))

(defn register-keyspace [ks-name cluster-cfg]
  (unregister-keyspace ks-name)
  (swap! keyspace-registry
         update-in [(keyword ks-name)]
         (fn [cfg]
           (let [ctx (build-keyspace-ctx ks-name cluster-cfg)]
             (.start ctx)
             (assoc cfg
               :context ctx
               :client  (.getClient ctx))))))

(defn lookup-keyspace [ks-name]
  (get-in @keyspace-registry [(keyword ks-name)]))

(defn keyspace-client [ks-name]
  (if-let [client (:client (lookup-keyspace ks-name))]
    client
    (do
      (register-keyspace ks-name cluster-config)
      (lookup-keyspace ks-name))))

(defn create-keyspace [ks-name & [opts]]
  (.createKeyspace
   (keyspace-client ks-name)
   {"strategy_options" (merge {"replication_factor" "1"} (:strategy-options opts))
    "strategy_class"   (or (:strategy-class opts) "SimpleStrategy")}))

(defn identifier->ks-client [id]
  (keyspace-client (keyword (namespace id))))

(defonce column-family-registry (atom {}))

(defn lookup-cf [cf-name]
  (get @column-family-registry (keyword cf-name)))

(defn register-cf [cf-name & [config]]
  (let [key-serializer (get-serializer-fn (:key-serializer config))
        col-serializer (get-serializer-fn (:col-serializer config))]
    (swap! column-family-registry assoc
           (keyword cf-name) (ColumnFamily/newColumnFamily (name cf-name) (key-serializer) (col-serializer)))))

(defonce cf-schema-column-families
  (ColumnFamily/newColumnFamily
   "schema_columnfamilies"
   (make-serializer :string)
   (make-serializer :string)))


(defn exec-cql [ks cf stmt & binds]
  (.execute
   (reduce (fn [q bind]
             (cond (string? bind)
                   (.withStringValue q bind)

                   (integer? bind)
                   (.withIntegerValue q bind)

                   (float? bind)
                   (.withFloatValue q bind)

                   :default
                   (.withFloatValue q bind)))
           (.. ks
               (prepareQuery cf)
               (withCql stmt)
               (asPreparedStatement))
           binds)))


(defn schema-column-family-info [keyspace table-name]
  (let [res (first
             (.getRows
              (.getResult
               (exec-cql
                (keyspace-client :system) cf-schema-column-families
                "select key_validator, default_validator, comparator
                   from system.schema_columnfamilies
                  where keyspace_name = ?
                    and columnfamily_name = ?"
                (.getKeyspaceName keyspace)
                table-name))))]
    {:key-serializer    (serializer-short-name (.. res (getColumns) (getColumnByName "key_validator") (getStringValue)))
     :default-validator (serializer-short-name (.. res (getColumns) (getColumnByName "default_validator") (getStringValue)))
     :col-serializer    (serializer-short-name (.. res (getColumns) (getColumnByName "comparator") (getStringValue)))}))

(defn ensure-column-def [keyspace table-name]
  (when-not (lookup-cf table-name)
    (register-cf table-name (schema-column-family-info keyspace table-name))))


;; NB: this uses old thrift API, may just get rid of it in favor of
;; create-table, which uses CQL3
(defn create-column-family
  ([ks cf-name]
     (create-column-family ks cf-name nil))
  ([ks cf-name opts]
     (.createColumnFamily ks (get @column-family-registry (keyword cf-name)) opts)))

(comment

  (def cpool (.getConnectionPool context))

  (def hosts (.getActivePools cpool))

  (first hosts)
  (.getActiveConnectionCount (first hosts))
  (.getIdleConnectionCount (first hosts))
  (.getBlockedThreadCount (first hosts))

  (def host (.getHost (first hosts)))

  (def monitor (.getConnectionPoolMonitor context))

  (.getConnectionReturnedCount monitor)
  (.getConnectionBorrowedCount monitor)

  (.getOperationSuccessCount monitor)
  (.getOperationFailureCount monitor)
  (.getOperationTimeoutCount monitor)

)


