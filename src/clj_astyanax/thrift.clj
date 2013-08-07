(ns clj-astyanax.thrift
)


(defn put-value [ks cf-name row-key col-name col-val]
  (.. ks
      (prepareColumnMutation (cf cf-name) row-key col-name)
      (putValue col-val nil)
      (execute)))


