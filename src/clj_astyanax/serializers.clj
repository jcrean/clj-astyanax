(ns clj-astyanax.serializers
  (:import
   [com.netflix.astyanax.serializers UUIDSerializer StringSerializer TimeUUIDSerializer Int32Serializer CompositeSerializer]))

(defn serializer-short-name [val]
  (let [prefix "org.apache.cassandra.db.marshal."]
    (.substring val (count prefix))))

(defn get-serializer-fn [sid]
  (cond (nil? sid)
        #(StringSerializer/get)

        (or (= :string sid)
            (= "UTF8Type" sid))
        #(StringSerializer/get)

        (or (= :uuid sid)
            (= "UUIDType" sid))
        #(UUIDSerializer/get)

        (or (= :time-uuid sid)
            (= "TimeUUIDType" sid))
        #(TimeUUIDSerializer/get)

        (or (= :int32 sid)
            (= "Int32Type" sid))
        #(Int32Serializer/get)

        (or (= :composite sid)
            (.startsWith sid "CompositeType"))
        #(CompositeSerializer/get)

        :default
        #(StringSerializer/get)))

(defn make-serializer [type]
  ((get-serializer-fn type)))