(ns ruuvi-server.database.event-dao
  (:use korma.db)
  (:use korma.core)
  (:use ruuvi-server.database.entities)
  (:use [clojure.tools.logging :only (debug info warn error)])
  (:require [ruuvi-server.util :as util])
  (:import org.joda.time.DateTime)
  )

;; private functions
(defn- to-sql-timestamp [date]
  (if date
    (java.sql.Timestamp. (.getMillis date))
    nil))
  
(defn- current-sql-timestamp []
  (to-sql-timestamp (org.joda.time.DateTime)))

;; public functions
(defn get-trackers [ids]
  (select tracker
          (where (in :id ids))))

(defn get-tracker-by-code [tracker-code]
  (first (select tracker
                 (where {:tracker_code tracker-code}))))

(defn get-tracker-by-code! [tracker-code & tracker-name]
  (let [existing-tracker (get-tracker-by-code tracker-code)]
    (if existing-tracker
      existing-tracker
      (insert tracker (values {:tracker_code tracker-code
                               :name (or tracker-name tracker-code)}))
      )))

(defn get-extension-type-by-id [id]
  (first (select event-extension-type
                 (where {:name id}))))


(defn get-extension-type-by-name [type-name]
  (first (select event-extension-type
                 (where {:name (str (name type-name))}))))

(defn get-extension-type-by-name! [type-name]
  (let [existing-extension-type (get-extension-type-by-name type-name)]
    (if existing-extension-type
      existing-extension-type
      (insert event-extension-type (values {:name (str (name type-name))
                                            :description "Autogenerated"}))
      )))

(defn get-event [event_id]
  (first (select event
                 (with tracker)
                 (with event-location)
                 (with event-extension-value)
                 (where {:id event_id})))
  )

(defn search-events 
  "Search events: criteria is a map that can contain following keys.
- :storeTimeStart <DateTime>, find events that are created (stored) to database later than given time (inclusive).
- :storeTimeDnd <DateTime>, find events that are created (stored) to database earlier than given time (inclusive).
- :eventTimeStart <DateTime>, find events that are created in tracker later than given time (inclusive).
- :eventTimeEnd <DateTime>, find events that are created in tracker earlier than given time (inclusive).
- :maxResults <Integer>, maximum number of events. Default and maximum is 50.
TODO calculates milliseconds wrong (12:30:01.000 is rounded to 12:30:01 but 12:30:01.001 is rounded to 12:30:02)
TODO make maxResults default configurable.
"
  [criteria]

  (let [event-start (to-sql-timestamp (:eventTimeStart criteria))
        event-end (to-sql-timestamp (:eventTimeEnd criteria))
        store-start (to-sql-timestamp (:storeTimeStart criteria))
        store-end (to-sql-timestamp (:storeTimeEnd criteria))
        tracker-ids (:trackerIds criteria)
        max-result-count 50
        max-results (:maxResults criteria max-result-count)
        result-limit (min max-result-count max-results)

        tracker-ids-crit (when tracker-ids {:tracker_id ['in tracker-ids]})
        event-start-crit (when event-start {:event_time ['>= event-start]})
        event-end-crit (when event-end {:event_time ['<= event-end]})
        store-start-crit (when store-start {:created_on ['>= store-start]})
        store-end-crit (when store-end {:created_on ['<= store-end]})
        conditions (filter identity (list event-start-crit event-end-crit tracker-ids-crit))
        ]
    (if (not (empty? conditions))
      (let [results (select event
                            (with event-location)
                            (with event-extension-value (fields :value)
                                  (with event-extension-type (fields :name)))              
                            (where (apply and conditions))
                            (limit result-limit)) ]
        results
        )
      '() )))

(defn get-events [ids]
  (select event
          (with event-location)
          (with event-extension-value
                (with event-extension-type (fields :name)))
          (where (in :id ids))))

(defn get-all-events []
  (select event
          (with event-location)
          (with event-extension-value)
          ))

(defn get-all-trackers []
  (select tracker)
  )

(defn get-tracker [id]
  ;; TODO support also fetching with tracker_indentifier?
  (first (select tracker
                 (where {:id id}))))  

(defn- update-tracker-latest-activity [id]
  (update tracker
          (set-fields {:latest_activity (java.sql.Timestamp. (System/currentTimeMillis)) })
          (where {:id id})))

(defn create-tracker [code name shared-secret]
  (info "Create new tracker" name "(" code ")")
  (insert tracker (values
                   {:tracker_code code
                    :shared_secret shared-secret
                    :name name})))

(defn create-event [data]
  (let [tracker (get-tracker-by-code! (:tracker_code data))]
    (transaction
     (update-tracker-latest-activity (tracker :id))
     (let [extension-keys (filter (fn [key]
                                    (.startsWith (str (name key)) "X-"))
                                  (keys data))
           latitude (:latitude data)
           longitude (:longitude data)
           
           event-entity (insert event (values
                                       {:tracker_id (tracker :id)
                                        :event_time (or (to-sql-timestamp (:event_time data))
                                                        (current-sql-timestamp) )
                                        }))]
       
       (if (and latitude longitude)
         (insert event-location (values
                                 {:event_id (:id event-entity)
                                  :latitude latitude
                                  :longitude longitude
                                  :accuracy (:accuracy data)
                                  :satellite_count (:satellite_count data)
                                  :altitude (:altitude data)}))
         )
       
       (doseq [key extension-keys]
         (insert event-extension-value
                 (values
                  {:event_id (:id event-entity)
                   :value (data key)
                   :event_extension_type_id (:id (get-extension-type-by-name! key))
                   }
                  )))
       event-entity
       )))
  )
