(ns ruuvi-server.database.pool
  (:import [org.apache.tomcat.jdbc.pool DataSource])
  (:import [com.jolbox.bonecp BoneCPDataSource ])
  (:use lobos.connectivity)
  )

(defn create-tomcat-connection-pool [spec]
  (let [connection-pool (doto (DataSource.)
                          (.setDriverClassName (spec :classname))
                          (.setUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
                          (.setUsername (:user spec))
                          (.setPassword (:password spec))
                          (.setMaxActive 80)
                          (.setMaxIdle 50)
                          )]
     {:datasource connection-pool}
           ))

(defn create-bonecp-connection-pool [spec]
  (let [connection-pool (doto (BoneCPDataSource.)
                          (.setDriverClass (spec :classname))
                          (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
                          (.setUsername (:user spec))
                          (.setPassword (:password spec))
                          (.setPartitionCount 3)
                          (.setMaxConnectionsPerPartition 26)
                          (.setStatementsCacheSize 100)
                          )]
     {:datasource connection-pool}
  ))