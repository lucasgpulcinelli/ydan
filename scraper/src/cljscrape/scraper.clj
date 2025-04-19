(ns cljscrape.scraper
  (:require [cheshire.core :as json]
            [cljscrape.utils :as utils]
            [cljscrape.constants :as consts]
            [cljscrape.queue :as q]
            [cljscrape.store :as store]
            [cljscrape.transforms :as trans])
  (:import [org.slf4j LoggerFactory]
           [java.time.format DateTimeFormatter]))

(def logger (LoggerFactory/getLogger "scraper"))

(defn scrape-id [id]
  (let
   [kind (utils/kind id)
    rename-map (kind consts/json-renamemaps)
    transform-map (kind trans/transforms)]
    (try
      (as-> id v
        (do
          (.info logger "started scraping {} {}" kind v)
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
          (.info logger "scraped {} {} successfully"  kind (get v "id"))
          v))
      (catch Exception e
        (.error logger "error occurred during scraping of {} {}: {} {}"
                kind id
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

(defn cutoff-scrape-filter [entry]
  (and
   (or
    (nil? (get "views" entry))
    (>= consts/minimum-views (get "views" entry)))
   (or
    (nil? (get "uploaded_date" entry))
    (>= consts/minimum-year
        (.getYear
         (java.time.LocalDate/parse
          (get "uploaded_date" entry)
          (DateTimeFormatter/ofPattern "yyyy/MM/dd")))))))

(defn get-recommendations [entry]
  (let [scrape-result (:data entry)]
    (concat
     (map
      #(get % "channel_id")
      (filter
       #(and (not (nil? (get % "channel_id"))) (cutoff-scrape-filter %))
       (get scrape-result "recommendations")))
     (map
      #(get % "id")
      (filter
       #(and (not (nil? (get % "id"))) (cutoff-scrape-filter %))
       (get scrape-result "videos")))
     (filter
      #(not (nil? %))
      (get scrape-result "related_channels")))))

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
          (.warn logger "we had {} failures, sleeping for {}s"
                 (count failed) (/ fail-sleep-time 1000))
          (Thread/sleep fail-sleep-time)))

    (if (empty? entries)
      (.warn logger "ended all possible scrapes")
      (do
        (store/save-entries successful)
        (q/mark-completed queue successful)
        (recur
         (if (empty? failed) consts/initial-fail-sleep-time (* fail-sleep-time 2))
         (->
          queue
          (q/put-entries failed)
          (q/put-entries recommendations)))))))
