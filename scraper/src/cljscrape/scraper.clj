(ns cljscrape.scraper
  (:require [cheshire.core :as json]
            [cljscrape.utils :as utils]
            [cljscrape.youtube :as yt]))

(defn get-match-from [pattern data]
  (let [matcher (.matcher pattern data)]
    (if (.find matcher)
      (json/decode (.group matcher 1))
      (throw (IllegalArgumentException. "no match found")))))

(defn get-prop [_ _])

(defn get-element [data element]
  (if (vector? element) (map #(get-prop %1 element) data) (get data element)))

(defn get-prop [data path]
  (reduce get-element data path))

(defn rename-props [_ _])

(defn get-and-rename-prop [data path]
  (if (map? (last path))
    (map (partial rename-props (last path)) (get-prop data (drop-last path)))
    (get-prop data path)))

(defn rename-props [propmap data]
  (reduce-kv #(assoc %1 %2 (get-and-rename-prop data %3)) nil propmap))

(defn transform-props [transform-map data]
  (into
   {}
   (map
    #(vector
      (first %1)
      (if (second %1) ((get transform-map (first %1) identity) (second %1)) (second %1)))
    data)))

(defn scrape-video [id]
  (let
   [rename-map (:video yt/properties)
    transform-map (:video yt/transforms)]
    (as-> id v
      (yt/videoid2url v)
      (utils/request v :string)
      (map #(get-match-from %1 v) yt/video-patterns)
      (reduce merge v)
      (rename-props rename-map v)
      (assoc v "thumbnail" (yt/thumbnailurl (get v "id")))
      (transform-props transform-map v)
      (do (printf "scraped video %s successfully\n" (get v "id")) v))))

(defn scrape-channel [id]
  (let
   [rename-map (:channel yt/properties)
    transform-map (:channel yt/transforms)]
    (as-> id v
      (yt/channelid2url v)
      (yt/list-videos v)
      (map json/decode v)
      (map (partial rename-props rename-map) v)
      (map (partial transform-props transform-map) v)
      (do (printf "scraped channel %s successfully\n" id) v))))

