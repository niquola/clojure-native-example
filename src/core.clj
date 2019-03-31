(ns core
  (:require [org.httpkit.server :as server]
            [cheshire.core :as json])
  (:import pgnio.Config
           pgnio.ConnectionIo
           pgnio.QueryBuildConnection
           pgnio.QueryMessage
           pgnio.Connection)
  (:gen-class))

(set! *warn-on-reflection* true)

(def cfg (doto (pgnio.Config.)
           (.hostname (or (System/getenv "PGHOST") "localhost"))
           (.username (or (System/getenv "PGUSER") "postgres"))
           (.password (or (System/getenv "PGPASSWORD") "postgres"))
           (.port     (Integer/parseInt (or (System/getenv "PGPORT") "5436")))))

(defn columns [^pgnio.QueryMessage$Row r]
  (->> (.columns ^pgnio.QueryMessage$RowMeta (.meta ^pgnio.QueryMessage$Row r))
       (reduce 
        (fn [acc ^pgnio.QueryMessage$RowMeta$Column c]
          (let [nm (keyword (.name c))]
            (assoc acc nm
                   {:index (.index c)
                    :oid (.tableOid c)
                    :column-attr-number (.columnAttributeNumber c)
                    :dt-oid (.dataTypeOid c)
                    :dt-size (.dataTypeSize c)
                    :dt-mod (.typeModifier c)
                    :text? (.textFormat c)
                    :array (.arrayParent c)}))
          ) {})))

(defmulti decode (fn [{dt-oid :dt-oid} ^bytes s] dt-oid))

;; jsonb
(defmethod decode
  3802
  [oid ^bytes s]
  (when s
    (json/parse-string (String. s) keyword)))

(defmethod decode :default
  [oid ^bytes s]
  (String. s))

(defn parse-rows [rows]
  (when-let [r (first rows)]
    (let [cols (columns r)
          parser (fn [^pgnio.QueryMessage$Row r]
                   (let [^bytes raw (.raw r)]
                     (->> cols
                          (reduce
                           (fn [acc [nm col]]
                             (if-let [bs (nth raw (:index col))]
                               (assoc! acc nm (decode col bs))
                               acc)
                             ) (transient {}))
                          (persistent!))))]
      (mapv parser rows))))

(defn query [^pgnio.QueryReadyConnection conn ^String query]
  (-> (.simpleQueryRows  conn query)
      deref
      parse-rows))

(defn handler [{db :db :as ctx} req]
  {:status 200
   :headers {"content-type" "application/json"}
   :body (json/generate-string (query db "select * from information_schema.tables"))})

(defn connection [^pgnio.Config cfg]
  (let [^pgnio.Connection$Startup i @(pgnio.Connection/init cfg)
        ^pgnio.QueryReadyConnection c @(.auth i)]
    c))

(defn start []
  (println "Start server")
  (let [conn (connection cfg)
        srv  (server/run-server (fn [req] (#'handler {:db conn} req))
                                {:port 8585})]
    {:web srv
     :db conn}))

(defn close [conn]
  @(.terminate ^pgnio.Connection conn))

(defn stop [state]
  (when-let [srv (:web state)]
    (srv))
  (when-let [db (:db state)]
    @(.terminate ^pgnio.Connection db)))

(defn -main [& [args]]
  (start))

(comment

  (def db (connection cfg))

  (close db)
  db

  (time
   (count (query db "select * from information_schema.tables")))

  (def tps (query db "select oid, * from pg_catalog.pg_type "))

  (def state (start))

  (stop state)

  )
