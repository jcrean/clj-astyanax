(ns clj-astyanax.ddl.helpers
  (:require
   [clojure.string :as str]))

(defn safe-name [s]
  (str/replace (name s) #"[^a-zA-Z0-9]" "_"))

(defn comma-join [x]
  (str/join "," x))

(defn space-join [x]
  (str/join " " x))
