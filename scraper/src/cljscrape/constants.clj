(ns cljscrape.constants
  (:import [java.util.regex Pattern]))

(def db-user (or (System/getenv "POSTGRES_USER") "postgres"))

(def db-pass (or (System/getenv "POSTGRES_PASS") "postgres"))

(def db-url (or (System/getenv "POSTGRES_URL") "jdbc:postgresql://localhost:5432/"))

(def minio-user (or (System/getenv "MINIO_USER") "minio-ydan-insecure"))

(def minio-pass (or (System/getenv "MINIO_PASS") "minio-ydan-insecure"))

(def minio-url (or (System/getenv "MINIO_URL") "http://localhost:9000"))

(def minio-bucket (or (System/getenv "MINIO_BUCKET") "ydan"))

(def initial-fail-sleep-time (Integer/parseInt (or (System/getenv "INITIAL_FAIL_TIME") "1000")))

(def partition-amount (Integer/parseInt (or (System/getenv "PARTITION_AMOUNT") "15")))

(def minimum-views (Integer/parseInt (or (System/getenv "MINIMUM_VIEWS") "1000000")))

(def minimum-year (Integer/parseInt (or (System/getenv "MINIMUM_YEAR") "2020")))

(def json-patterns
  {:video [(Pattern/compile "var ytInitialPlayerResponse = (.*);</script>")
           (Pattern/compile "var ytInitialData = (.*);</script>")]
   :channel [(Pattern/compile "var ytInitialData = (.*);</script>")]})

(def json-renamemaps
  {:video
   {"duration_s" ["videoDetails" "lengthSeconds"]
    "is_live" ["videoDetails" "isLiveContent"]
    "title" ["videoDetails" "title"]
    "description" ["videoDetails" "shortDescription"]
    "channel_id" ["videoDetails" "channelId"]
    "views" ["videoDetails" "viewCount"]
    "keywords" ["videoDetails" "keywords"]
    "id" ["videoDetails" "videoId"]
    "thumbnail" ["videoDetails" "videoId"]
    "likes" ["contents" "twoColumnWatchNextResults" "results" "results" "contents" 0 "videoPrimaryInfoRenderer" "videoActions" "menuRenderer" "topLevelButtons" 0 "segmentedLikeDislikeButtonViewModel" "likeButtonViewModel" "likeButtonViewModel" "toggleButtonViewModel" "toggleButtonViewModel" "defaultButtonViewModel" "buttonViewModel" "accessibilityText"]
    "uploaded_date" ["contents" "twoColumnWatchNextResults" "results" "results" "contents" 0 "videoPrimaryInfoRenderer" "dateText" "simpleText"]
    "channel_subscribers" ["contents" "twoColumnWatchNextResults" "results" "results" "contents" 1 "videoSecondaryInfoRenderer" "owner" "videoOwnerRenderer" "subscriberCountText" "simpleText"]
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
   :channel
   {"id" ["metadata" "channelMetadataRenderer" "externalId"]
    "videos" ["metadata" "channelMetadataRenderer" "externalId"]
    "name" ["microformat" "microformatDataRenderer" "title"]
    "related_channels" ["contents" "twoColumnBrowseResultsRenderer" "tabs" 0 "tabRenderer" "content" "sectionListRenderer" "contents"
                        ["itemSectionRenderer" "contents" 0 "shelfRenderer" "content" "horizontalListRenderer" "items"
                         ["gridChannelRenderer" "channelId"]]]
    "subscribers" ["header" "pageHeaderRenderer" "content" "pageHeaderViewModel" "metadata" "contentMetadataViewModel" "metadataRows" 1 "metadataParts" 0 "text" "content"]
    "video_count" ["header" "pageHeaderRenderer" "content" "pageHeaderViewModel" "metadata" "contentMetadataViewModel" "metadataRows" 1 "metadataParts" 1 "text" "content"]
    "description" ["metadata" "channelMetadataRenderer" "description"]}})

(def channel-video-renamemap
  {"id" ["id"]
   "views" ["view_count"]
   "uploaded_date_unix" ["release_timestamp"]
   "channel_id" ["playlist_channel_id"]
   "duration" ["duration"]})

(def final-recommendation-properties ["id" "views" "uploaded_date" "channel_id" "duration"])

