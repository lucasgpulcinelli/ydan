(ns cljscrape.scraper
  (:require [cheshire.core :as json]
            [cljscrape.utils :as utils]
            [cljscrape.constants :as consts]
            [cljscrape.transforms :as trans]))

(defn scrape [id]
  (let
   [kind (utils/kind id)
    rename-map (kind consts/json-renamemaps)
    transform-map (kind trans/transforms)]
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
      (do (printf "scraped %s %s successfully\n" kind (get v "id")) v))))
