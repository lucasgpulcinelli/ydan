(ns cljscrape.scraper
  (:require [cheshire.core :as json]
            [cljscrape.utils :as utils]
            [cljscrape.constants :as consts]
            [cljscrape.queue :as q]
            [cljscrape.store :as store]
            [cljscrape.transforms :as trans]))

(defn scrape-id [id]
  (let
   [kind (utils/kind id)
    rename-map (kind consts/json-renamemaps)
    transform-map (kind trans/transforms)]
    (try
      (as-> id v
        (do
          (println "started scraping" kind v)
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
  (let [[queue entries] (q/get-next-entries queue)
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
        (store/save-entries successful)
        (q/mark-completed queue successful)
        (recur
         (if (empty? failed) consts/initial-fail-sleep-time (* fail-sleep-time 2))
         (->
          queue
          (q/put-entries failed)
          (q/put-entries recommendations)))))))
