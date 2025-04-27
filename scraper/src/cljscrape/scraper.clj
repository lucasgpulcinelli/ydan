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
        (if (and (= kind :video) (nil? (get v "videoDetails")))
          (throw (IllegalArgumentException. "video has no details property"))
          v)
        (utils/rename-props rename-map v)
        (utils/transform-props transform-map v)
        (assoc v "time_of_scraping_unix" (quot (System/currentTimeMillis) 1000))
        (do
          (and (nil? (get v "id")) (throw (IllegalArgumentException. "result has no ID")))
          (.info logger "scraped {} {} successfully"  kind id)
          v))
      (catch Exception e
        (.error logger "error occurred during scraping of {}: {}"
                (format "%s %s" kind id)
                (format "%s %s" (class e) (.getMessage e)))
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

(defn relevant? [entry]
  (and
   (or
    (nil? (get entry "views"))
    (<= consts/minimum-views (get entry "views")))
   (or
    (nil? (get entry "uploaded_date"))
    (<= consts/minimum-year
        (.getYear
         (java.time.LocalDate/parse
          (get entry "uploaded_date")
          (DateTimeFormatter/ofPattern "yyyy/MM/dd")))))))

(defn get-recommendations [entry]
  (filter
   #(not (nil? %))
   (if (= :video (utils/kind (:id entry)))
     (let [scrape-result (:data entry)
           important-recommendations (filter
                                      #(relevant? %)
                                      (get scrape-result "recommendations"))
           important-channels (map
                               #(get % "channel_id")
                               important-recommendations)]
       important-channels)
     (let [scrape-result (:data entry)
           related-channels (get scrape-result "related_channels")
           channel-videos (get scrape-result "videos")
           channel-video-ids (map #(get % "id") channel-videos)]
       (concat channel-video-ids related-channels)))))

(defn scrape-loop [fail-sleep-time queue]
  (let [[queue entries] (q/get-next-entries queue)
        results (pmap scrape-entry entries)
        {successful true failed false} (group-by #(= "completed" (:state %)) results)
        recommendations (->> successful
                             (map get-recommendations)
                             (flatten)
                             (distinct)
                             (map make-entry))]
    (and (empty? successful)
         (do
           (.warn logger "we had {} failures, sleeping for {}s"
                  (count failed) (/ fail-sleep-time 1000))
           (Thread/sleep fail-sleep-time)))

    (if (empty? entries)
      (.warn logger "ended all possible scrapes")
      (do
        (store/save-entries successful)
        (q/put-entries queue recommendations)
        (q/update-entries queue results)
        (recur
         (if (empty? successful) (* fail-sleep-time 2) consts/initial-fail-sleep-time)
         queue)))))
