(ns cljscrape.queue
  (:require
   [clojure.string :as string]
   [cljscrape.utils :as utils]))

(defn entries-from-resultset [rs]
  (loop [results []]
    (if (.next rs)
      (recur
       (conj results
             {:id (string/trim (.getString rs "id"))
              :tries (.getInt rs "tries")
              :state "running"}))
      results)))

(defn entries-from-sql [st sql]
  (let [rs (.executeQuery st sql)
        results (entries-from-resultset rs)]
    (.close rs)
    results))

(defprotocol Queue
  (get-next-entries [this])
  (update-entries [this entries])
  (put-entries [this entries]))

(defrecord PostgresQueue [conn]
  Queue

  (get-next-entries [this]
    (let [st (.createStatement conn)
          video-results (entries-from-sql st "SELECT * FROM ydan.random_n_entries(10, 'video'::ydan.scrape_kind);")
          results (if (= 0 (count video-results))
                    (entries-from-sql st "SELECT * FROM ydan.random_n_entries(10, 'channel'::ydan.scrape_kind);")
                    video-results)]
      (.close st)
      [this, results]))

  (put-entries [this entries]
    (let [st
          (.prepareStatement
           conn
           "INSERT INTO ydan.entries (id, tries, state, kind)
            VALUES (?, ?, ? :: ydan.scrape_state, ? :: ydan.scrape_kind)
            ON CONFLICT (id) DO NOTHING
           ")]
      (doall
       (map (fn [entry]
              (.setString st 1 (:id entry))
              (.setInt st 2 (:tries entry))
              (.setString st 3 (:state entry))
              (.setString st 4 (-> entry (:id) (utils/kind) (.toString) (.substring 1)))
              (.addBatch st))
            entries))
      (.clearParameters st)
      (.executeBatch st)
      (.close st)))

  (update-entries [this entries]
    (let [st
          (.prepareStatement
           conn
           "UPDATE ydan.entries
            SET tries = ?, state = ? :: ydan.scrape_state
            WHERE id = ?")]
      (doall
       (map (fn [entry]
              (.setInt st 1 (:tries entry))
              (.setString st 2 (:state entry))
              (.setString st 3 (:id entry))
              (.addBatch st))
            entries))
      (.clearParameters st)
      (.executeBatch st)
      (.close st))))

