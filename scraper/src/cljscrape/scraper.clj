(ns cljscrape.scraper
  (:require [cheshire.core :as json]
            [clojure.string :as string]
            [cljscrape.utils :as utils]
            [cljscrape.constants :as consts]
            [cljscrape.transforms :as trans]))

(defn get-next-entries [queue]
  (let [st (.createStatement queue)
        rs (.executeQuery
            st "SELECT * FROM ydan.random_n_entries(10);")
        results (loop [results []]
                  (if (.next rs)
                    (recur
                     (conj results
                           {:id (string/trim (.getString rs "id"))
                            :tries (.getInt rs "tries")
                            :state "running"}))
                    results))]
    (.close rs)
    (.close st)
    [queue, results]))

(defn put-entries [queue entries]
  (let [st
        (.prepareStatement
         queue
         "INSERT INTO ydan.entries (id, tries, state)
          VALUES (?, ?, ? :: ydan.scrape_state)
          ON CONFLICT (id) DO
          UPDATE
          SET tries = EXCLUDED.tries, state = EXCLUDED.state")]
    (doall
     (map (fn [entry]
            (.setString st 1 (:id entry))
            (.setInt st 2 (:tries entry))
            (.setString st 3 (:state entry))
            (.addBatch st))
          entries))
    (.clearParameters st)
    (.executeBatch st)
    (.close st))
  queue)

(defn save-entry [entry]
  (let [id (:id entry)]
    (as-> entry v
      (:data v)
      (if (utils/video? id)
        (let [f (java.io.File. (format "cljscrape/%s.jpeg" id))
              is (java.io.FileOutputStream. f)]
          (.write is (get v "thumbnail"))
          (.close is)
          (spit (format "cljscrape/%s.xml" id) (get v "captions"))
          (-> v (dissoc "thumbnail") (dissoc "captions")))
        v)
      (json/encode v)
      (spit (format "cljscrape/%s.json" id) v))))

(defn save-entries [queue entries]
  (put-entries queue entries)
  (doall (map save-entry entries)))

(defn scrape-id [id]
  (let
   [kind (utils/kind id)
    rename-map (kind consts/json-renamemaps)
    transform-map (kind trans/transforms)]
    (try
      (as-> id v
        (do
          (println "started scraping %s %s" kind v)
          v)
        ((kind utils/id2url) v)
        (utils/request v :string)
        (map #(utils/get-match-from %1 v) (kind consts/json-patterns))
        (map json/decode v)
        (reduce merge v)
        (utils/rename-props rename-map v)
        (utils/transform-props transform-map v)
        (assoc v "time_of_scraping_unix" (quot (System/currentTimeMillis) 1000))
        (do
          (println "scraped" kind (get v "id") "successfully")
          v))
      (catch Exception e
        (println "error occurred during scraping of"
                 kind id ":"
                 (class e) (.getMessage e))
        nil))))

(defn make-entry [id] {:tries 0 :id id :state "pending"})

(defn scrape-entry [entry]
  (let [res (scrape-id (:id entry))]
    {:tries (inc (:tries entry))
     :id (:id entry)
     :state (if (nil? res)
              (if (>= (:tries entry) 9) "failed" "pending")
              "completed")
     :data res}))

(defn get-recommendations [entry]
  (let [scrape-result (:data entry)]
    (concat
     (filter #(not (nil? %)) (map #(get % "channel_id") (get scrape-result "recommendations")))
     (filter #(not (nil? %)) (map #(get % "id") (get scrape-result "videos")))
     (filter #(not (nil? %)) (get scrape-result "related_channels")))))

(defn scrape-loop [fail-sleep-time queue]
  (let [[queue entries] (get-next-entries queue)
        results (pmap scrape-entry entries)
        {successful true failed false} (group-by #(= "completed" (:state %)) results)
        recommendations (->> successful
                             (map get-recommendations)
                             (flatten)
                             (distinct)
                             (map make-entry))]
    (or (empty? failed)
        (do
          (println "we had" (count failed) "failures, sleeping for"
                   (/ fail-sleep-time 1000) "s")
          (Thread/sleep fail-sleep-time)))

    (if (empty? entries)
      (println "ended all possible scrapes")
      (do
        (save-entries queue successful)
        (recur
         (if (empty? failed) 1000 (* fail-sleep-time 2))
         (->
          queue
          (put-entries failed)
          (put-entries recommendations)))))))
