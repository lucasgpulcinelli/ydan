(ns cljscrape.youtube
  (:require
   [clojure.string :as string]
   [cljscrape.utils :as utils]
   [clojure.java.shell :as sh])
  (:import
   [java.time.format DateTimeFormatter]
   [java.time LocalDateTime]
   [java.io IOException]
   [java.util.regex Pattern]))

(def video-patterns [(Pattern/compile "var ytInitialPlayerResponse = (.*);</script>")
                     (Pattern/compile "var ytInitialData = (.*);</script>")])

(def properties
  {:video
   {"duration_s" ["videoDetails" "lengthSeconds"]
    "is_live" ["videoDetails" "isLiveContent"]
    "title" ["videoDetails" "title"]
    "description" ["videoDetails" "shortDescription"]
    "channel_id" ["videoDetails" "channelId"]
    "views" ["videoDetails" "viewCount"]
    "keywords" ["videoDetails" "keywords"]
    "id" ["videoDetails" "videoId"]
    "likes" ["contents" "twoColumnWatchNextResults" "results" "results" "contents" 0 "videoPrimaryInfoRenderer" "videoActions" "menuRenderer" "topLevelButtons" 0 "segmentedLikeDislikeButtonViewModel" "likeButtonViewModel" "likeButtonViewModel" "toggleButtonViewModel" "toggleButtonViewModel" "defaultButtonViewModel" "buttonViewModel" "accessibilityText"]
    "uploaded_date" ["contents" "twoColumnWatchNextResults" "results" "results" "contents" 0 "videoPrimaryInfoRenderer" "dateText" "simpleText"]
    "channel_subscribers" ["contents" "twoColumnWatchNextResults" "results" "results" "contents" 1 "videoSecondaryInfoRenderer" "owner" "videoOwnerRenderer" "subscriberCountText" "simpleText"]
    "time_of_scraping_unix" ["frameworkUpdates" "entityBatchUpdate" "timestamp" "seconds"]
    "heatmap" ["frameworkUpdates" "entityBatchUpdate" "mutations" 0 "payload" "macroMarkersListEntity" "markersList" "markers"
               ["intensityScoreNormalized"]]
    "recommendations" ["contents" "twoColumnWatchNextResults" "secondaryResults" "secondaryResults" "results"
                       {"id_1" ["compactVideoRenderer" "videoId"]
                        "id_2" ["lockupViewModel" "rendererContext" "commandContext" "onTap" "innertubeCommand" "watchEndpoint" "videoId"]
                        "duration_1" ["compactVideoRenderer" "lengthText" "simpleText"]
                        "duration_2" ["lockupViewModel" "contentImage" "thumbnailViewModel" "overlays" 0 "thumbnailOverlayBadgeViewModel" "thumbnailBadges" 0 "thumbnailBadgeViewModel" "text"]
                        "views_1" ["compactVideoRenderer" "viewCountText" "simpleText"]
                        "views_2" ["lockupViewModel" "metadata" "lockupMetadataViewModel" "metadata" "contentMetadataViewModel" "metadataRows" 1 "metadataParts" 0 "text" "content"]
                        "uploaded_date_1" ["compactVideoRenderer" "publishedTimeText" "simpleText"]
                        "uploaded_date_2" ["lockupViewModel" "metadata" "lockupMetadataViewModel" "metadata" "contentMetadataViewModel" "metadataRows" 1 "metadataParts" 1 "text" "content"]
                        "channel_id_1" ["compactVideoRenderer" "shortBylineText" "runs" 0 "navigationEndpoint" "browseEndpoint" "browseId"]
                        "channel_id_2" ["lockupViewModel" "metadata" "lockupMetadataViewModel" "image" "decoratedAvatarViewModel" "rendererContext" "commandContext" "onTap" "innertubeCommand" "browseEndpoint" "browseId"]}]
    "chapters" ["playerOverlays" "playerOverlayRenderer" "decoratedPlayerBarRenderer" "decoratedPlayerBarRenderer" "playerBar" "multiMarkersPlayerBarRenderer" "markersMap" 0 "value" "chapters"
                {"title" ["chapterRenderer" "title" "simpleText"]
                 "start_ms" ["chapterRenderer" "timeRangeStartMillis"]}]
    "captions" ["captions" "playerCaptionsTracklistRenderer" "captionTracks"
                {"url" ["baseUrl"]
                 "translatable" ["isTranslatable"]
                 "language" ["languageCode"]
                 "name" ["name" "simpleText"]}]
    "family_friendly" ["microformat" "playerMicroformatRenderer" "isFamilySafe"]}
   :channel {"id" ["id"]
             "views" ["view_count"]
             "uploaded_date_unix" ["release_timestamp"]
             "channel_id" ["playlist_channel_id"]
             "duration" ["duration"]}})

(defn parse-upload-date [date-str]
  (let [input-formatter (DateTimeFormatter/ofPattern "MMM d, yyyy")
        output-formatter (DateTimeFormatter/ofPattern "yyyy/MM/dd")
        date (java.time.LocalDate/parse date-str input-formatter)]
    (.format date output-formatter)))

(defn parse-subscriber-count [s]
  (let [re #"(\d+(\.\d+)?)\s*(K|M|)\s*subscribers"
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

(defn parse-rec-duration [timestamp]
  (try
    (let [parts (map #(Integer/parseInt %) (clojure.string/split timestamp #":"))
          num-parts (count parts)]
      (cond
        (= num-parts 3) (+ (* (first parts) 3600) (* (second parts) 60) (last parts))
        (= num-parts 2) (+ (* (first parts) 60) (last parts))
        (= num-parts 1) (+ (first parts))
        :else timestamp))
    (catch NumberFormatException _ timestamp)))

(defn parse-rec-views [s]
  (let [re #"(\d+(?:,\d+)*) views"
        match (re-find re s)]
    (if match
      (->
       match
       (second)
       (string/replace "," "")
       (read-string))
      s)))

(defn parse-duration [duration-str]
  (let [pattern #"(\d+) (hour|day|week|month|year|minute|second)s? ago"
        matches (re-matches pattern duration-str)]
    (when matches
      (let [value (Integer/parseInt (second matches))
            unit (nth matches 2)]
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

(def rec-transform {"duration" parse-rec-duration
                    "views" parse-rec-views
                    "uploaded_date" parse-time-string})

(def final-rec-props ["id" "views" "uploaded_date" "channel_id" "duration"])

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
    final-rec-props)))

(defn parse-recommendation [rec]
  (into
   {}
   (map
    #(vector
      (first %1)
      (if (second %1) ((get rec-transform (first %1) identity) (second %1)) (second %1)))
    (merge-recommendation-props rec))))

(defn parse-recommendations [recs]
  (filter #(not (nil? (get %1 "id"))) (map parse-recommendation recs)))

(defn get-translated-caption [cap]
  (if cap
    (utils/request (str (get cap "url") (if (not= (get cap "language") "en") "&tlang=en" "")) :string)
    nil))

(defn choose-caption [caps]
  (->>
   caps
   (filter #(and (get % "translatable") (get % "name")))
   (filter #(.contains (get % "name") "auto-generated"))
   (first)))

(def transforms {:video {"captions" #(get-translated-caption (choose-caption %))
                         "uploaded_date" parse-upload-date
                         "channel_subscribers" parse-subscriber-count
                         "duration_s" read-string
                         "likes" parse-likes
                         "recommendations" parse-recommendations
                         "time_of_scraping_unix" read-string
                         "views" read-string
                         "thumbnail" #(utils/request %1 :bytes)}
                 :channel {}})

(defn videoid2url [id] (format "https://www.youtube.com/watch?v=%s&gl=US&hl=en" id))

(defn channelid2url [id] (format "https://www.youtube.com/channel/%s/videos" id))

(defn thumbnailurl [id] (format "https://i.ytimg.com/vi/%s/hqdefault.jpg" id))

(defn list-videos [channel-url]
  (let [res (sh/sh
             "yt-dlp" channel-url
             "--skip-download" "--dump-json" "--flat-playlist"
             "-I" "0:1000:1")]
    (if (= (:exit res) 0)
      (string/split-lines (:out res))
      (throw
       (IOException. (format "invalid return for yt-dlp: %s" (:err res)))))))
