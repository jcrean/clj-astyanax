(ns clj-astyanax.cql
  (:require
   [clojure.string :as str])
  (:use
   clj-astyanax.core
   clj-astyanax.ddl.helpers))

(defn create-table! [table-name col-defs table-opts]
  (let [pk (filter
            identity
            (if (vector? (:pk table-opts))
              (:pk table-opts)
              [(:pk table-opts)]))
        cols (partition 2 col-defs)]
    (when (empty? pk)
      (throw (RuntimeException. "Woah woah woah, you gotta supply a primary key with the :pk option")))
    (exec-cql (identifier->ks-client table-name) (lookup-cf (name table-name))
              (format "CREATE TABLE %s (%s, PRIMARY KEY (%s))"
                      (safe-name table-name)
                      (comma-join (map #(space-join (map safe-name %)) cols))
                      (comma-join (map safe-name pk))))))

(defn drop-table! [table-name]
  (exec-cql (identifier->ks-client table-name) (lookup-cf (name table-name))
            (format "DROP TABLE %s" (safe-name table-name))))

(defn table-op [table]
  (let [keyspace   (identifier->ks-client table)
        table-name (name table)]
    (def zzz [keyspace table-name])
    (ensure-column-def keyspace table-name)
    {:keyspace   keyspace
     :table-name table-name}))

(defn insert-row [table row]
  (let [{:keys [table-name keyspace]} (table-op table)
        insert-cols (keys row)]
    (def yy [table-name keyspace])
    (apply
     exec-cql
     keyspace (lookup-cf table-name)
     (format "insert into %s.%s (%s) values (%s)"
             (.getKeyspaceName keyspace) (name table-name)
             (str/join "," (map safe-name insert-cols))
             (str/join "," (repeat (count row) "?")))
     (map row insert-cols))))


(comment
  (register-cluster :test
                    {:cluster            "Test Cluster"
                     :seeds              "127.0.0.1:9160"
                     :cql-version        "3.0.0"
                     :target-cassandra   "1.2"
                     :port               9160
                     :max-conns-per-host 1})

  (with-cluster :test
    (create-keyspace :jctest))

  (with-cluster :test
    (create-table! :jctest/foo
                   [:pid   :int
                    :fname :varchar
                    :lname :varchar]
                   {:pk :pid}))

  (with-cluster :test
    (insert-row :jctest/foo {:pid (Integer. 123) :fname "jc" :lname "crean"}))

  )