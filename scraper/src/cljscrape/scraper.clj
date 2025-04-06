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

(defn scrape [url]
  (let
   [rename-map ((yt/kind url) yt/properties)
    transform-map ((yt/kind url) yt/transforms)]
    (as-> url v
      (utils/request v :string)
      (map #(get-match-from %1 v) yt/json-patterns)
      (reduce merge v)
      (rename-props rename-map v)
      (assoc v "thumbnail" (yt/thumbnailurl (get v "id")))
      (transform-props transform-map v)
      (do (printf "scraped %s successfully\n" (get v "id")) v))))

