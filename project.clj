(defproject clj-astyanax "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dev-dependencies [[swank-clojure "1.4.2"]]
  :dependencies [[org.clojure/clojure                     "1.4.0"]
                 [com.netflix.astyanax/astyanax-core      "1.56.42"]
                 [com.netflix.astyanax/astyanax-thrift    "1.56.42"
                  :exclusions [org.apache.cassandra/cassandra-thrift]]
                 [com.netflix.astyanax/astyanax-cassandra "1.56.42"
                  :exclusions [org.apache.cassandra/cassandra-all]]
                 [org.apache.cassandra/cassandra-all      "1.2.2"]
                 [org.apache.cassandra/cassandra-thrift   "1.2.2"]])
