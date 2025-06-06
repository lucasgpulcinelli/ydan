(ns cljscrape.transforms
  (:require [clojure.string :as string]
            [cljscrape.constants :as consts]
            [cljscrape.utils :as utils]
            [clojure.java.shell :as sh]
            [cheshire.core :as json])
  (:import
   [java.time.format DateTimeFormatter]
   [java.io IOException]
   [java.time LocalDateTime]))

(defn parse-upload-date [date-str]
  (let [re #"(Premiere(d|s)? |Streamed live on |Started streaming on )?(.*)"
        match (re-find re date-str)
        input-formatter (DateTimeFormatter/ofPattern "MMM d, yyyy")
        output-formatter (DateTimeFormatter/ofPattern "yyyy/MM/dd")]
    (if match
      (.format (java.time.LocalDate/parse (nth match 3) input-formatter) output-formatter)
      nil)))

(defn parse-subscriber-count [s]
  (let [re #"(\d+(\.\d+)?)\s*(K|M|)\s*subscribers?"
        match (re-find re s)]
    (if match
      (let [num-str (nth match 1)
            suffix (nth match 3)
            number (read-string num-str)]
        (cond
          (= suffix "K") (* number 1000)
          (= suffix "M") (* number 1000000)
          :else number))
      s)))

(defn parse-likes [s]
  (let [re #"like this video along with (\d+(?:,\d+)*) other people"
        match (re-find re s)]
    (if match (-> match
                  (second)
                  (string/replace "," "")
                  (read-string))
        s)))

(defn parse-recommendation-duration [timestamp]
  (try
    (let [parts (map #(Integer/parseInt %) (clojure.string/split timestamp #":"))
          num-parts (count parts)]
      (cond
        (= num-parts 3) (+ (* (first parts) 3600) (* (second parts) 60) (last parts))
        (= num-parts 2) (+ (* (first parts) 60) (last parts))
        (= num-parts 1) (+ (first parts))
        :else timestamp))
    (catch NumberFormatException _ timestamp)))

(defn parse-views [s]
  (let [re #"(\d+(?:.\d+)*)\s*(K|M|B|)\s*(views|watching)?"
        match (re-find re s)]
    (if match
      (let [num-str (-> (nth match 1) (string/replace "," ""))
            suffix (nth match 2)
            number (read-string num-str)]
        (cond
          (= suffix "K") (* number 1000)
          (= suffix "M") (* number 1000000)
          (= suffix "B") (* number 1000000000)
          :else number))
      s)))

(defn parse-duration [duration-str]
  (let [pattern #"(Streamed )?(\d+) (hour|day|week|month|year|minute|second)s? ago"
        matches (re-matches pattern duration-str)]
    (when matches
      (let [value (Integer/parseInt (nth matches 2))
            unit (nth matches 3)]
        {:value value :unit unit}))))

(defn subtract-time [date {:keys [value unit]}]
  (case unit
    "second" (.minusSeconds date value)
    "minute" (.minusMinutes date value)
    "hour" (.minusHours date value)
    "day" (.minusDays date value)
    "week" (.minusWeeks date value)
    "month" (.minusMonths date value)
    "year" (.minusYears date value)))

(defn parse-time-string [time-str]
  (let [current-time (LocalDateTime/now)
        duration (parse-duration time-str)]
    (if duration
      (.format (subtract-time current-time duration) (DateTimeFormatter/ofPattern "yyyy/MM/dd"))
      time-str)))

(defn merge-recommendation-prop [rec prefix]
  (->>
   rec
   (filter #(and (.startsWith (first %1) prefix) (not (nil? (second %1)))))
   (first)
   (second)))

(defn merge-recommendation-props [rec]
  (into
   {}
   (map
    #(vector %1 (merge-recommendation-prop rec %1))
    consts/final-recommendation-properties)))

(def recommendation-transforms
  {"duration" parse-recommendation-duration
   "views" parse-views
   "uploaded_date" parse-time-string})

(defn parse-recommendation [rec]
  (into
   {}
   (map
    #(vector
      (first %1)
      (if (second %1)
        ((get recommendation-transforms (first %1) identity) (second %1))
        (second %1)))
    (merge-recommendation-props rec))))

(defn parse-recommendations [recs]
  (filter #(not (nil? (get %1 "id"))) (map parse-recommendation recs)))

(defn get-translated-caption [cap]
  (if cap [(get cap "language") (utils/request (get cap "url") :string)] nil))

(defn choose-caption [caps]
  (->>
   caps
   (filter #(get % "name"))
   (filter #(.contains (get % "name") "auto-generated"))
   (first)))

(defn list-videos [channel-id]
  (let [res (sh/sh
             "yt-dlp" ((:channel utils/id2url) (format "%s/videos" channel-id))
             "--skip-download" "--dump-json" "--flat-playlist"
             "-I" "0:1000:1")]
    (if (= (:exit res) 0)
      (->>
       res
       (:out)
       (string/split-lines)
       (map json/decode)
       (map (partial utils/rename-props consts/channel-video-renamemap)))
      (throw
       (IOException. (format "invalid return for yt-dlp: %s" (:err res)))))))

(defn parse-video-count [s]
  (let [re #"(\d+(\.\d+)?)\s*(K|M|)\s*videos?"
        match (re-find re s)]
    (if match
      (let [num-str (nth match 1)
            suffix (nth match 3)
            number (read-string num-str)]
        (cond
          (= suffix "K") (* number 1000)
          (= suffix "M") (* number 1000000)
          :else number))
      s)))

(defn merge-related-channels [vs]
  (filter #(not (nil? %)) (apply concat vs)))

(def transforms
  {:video {"captions" #(get-translated-caption (choose-caption %))
           "uploaded_date" parse-upload-date
           "channel_subscribers" parse-subscriber-count
           "duration_s" read-string
           "likes" parse-likes
           "recommendations" parse-recommendations
           "time_of_scraping_unix" read-string
           "views" parse-views
           "thumbnail" #(utils/request (utils/thumbnailurl %1) :bytes)}
   :channel {"videos" list-videos
             "video_count" parse-video-count
             "related_channels" merge-related-channels
             "subscribers" parse-subscriber-count}})

