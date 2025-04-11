(ns cljscrape.scraper
  (:require [cheshire.core :as json]
            [cljscrape.utils :as utils]
            [cljscrape.constants :as consts]
            [cljscrape.transforms :as trans]))

(defn get-next-entries [queue] [(rest queue), [(first queue)]])

(defn put-entries [queue entries]
  (let [filtered-entries (filter (fn [e] (reduce #(or %1 (= e %2)) queue)) entries)
        new-queue (concat queue filtered-entries)]
    new-queue))

(defn save-entry [entry]
  (let [id (:id entry)]
    (as-> entry v
      (:data v)
      (if (utils/video? id)
        (let [f (java.io.File. (format "/tmp/cljscrape/%s.jpeg" id))
              is (java.io.FileOutputStream. f)]
          (.write is (get v "thumbnail"))
          (.close is)
          (spit (format "/tmp/cljscrape/%s.xml" id) (get v "captions"))
          (-> v (dissoc "thumbnail") (dissoc "captions")))
        v)
      (json/encode v)
      (spit (format "/tmp/cljscrape/%s.json" id) v))))

(defn save-entries [entries]
  (doall (map save-entry entries)))

(defn scrape-id [id]
  (let
   [kind (utils/kind id)
    rename-map (kind consts/json-renamemaps)
    transform-map (kind trans/transforms)]
    (try
      (as-> id v
        (do (printf "started scraping %s %s\n" kind v) v)
        ((kind utils/id2url) v)
        (utils/request v :string)
        (map #(utils/get-match-from %1 v) (kind consts/json-patterns))
        (map json/decode v)
        (reduce merge v)
        (utils/rename-props rename-map v)
        (utils/transform-props transform-map v)
        (assoc v "time_of_scraping_unix" (quot (System/currentTimeMillis) 1000))
        (do (printf "scraped %s %s successfully\n" kind (get v "id")) v))
      (catch Exception e
        (printf "error occurred during scraping of %s %s: %s %s\n"
                kind id (-> e (:via) (get 0) (:type)) (:cause e))
        nil))))

(defn make-entry [id] {:tries 0 :id id :state "pending"})

(defn scrape-entry [entry]
  (let [res (scrape-id (:id entry))]
    {:tries (inc (:tries entry))
     :id (:id entry)
     :state (if (nil? res) :pending :completed)
     :data res}))

(defn get-recommendations [entry]
  (let [scrape-result (:data entry)]
    (concat
     (filter #(not (nil? %)) (map #(get % "channel_id") (get scrape-result "recommendations")))
     (filter #(not (nil? %)) (map #(get % "id") (get scrape-result "videos")))
     (filter #(not (nil? %)) (get scrape-result "related_channels")))))

(defn scrape-loop [fail-sleep-time queue]
  (let [[queue entries] (get-next-entries queue)
        results (map scrape-entry entries)
        {successful true failed false} (group-by #(= :completed (:state %)) results)
        recommendations (->> successful
                             (map get-recommendations)
                             (flatten)
                             (distinct)
                             (map make-entry))]
    (or (empty? failed)
        (do
          (printf "we had %d failures, sleeping for %d s\n"
                  (count failed) (/ fail-sleep-time 1000))
          (Thread/sleep fail-sleep-time)))

    (if (empty? entries)
      (println "ended all possible scrapes")
      (do
        (save-entries successful)
        (recur
         (if (empty? failed) 1000 (* fail-sleep-time 2))
         (->
          queue
          (put-entries failed)
          (put-entries recommendations)))))))
