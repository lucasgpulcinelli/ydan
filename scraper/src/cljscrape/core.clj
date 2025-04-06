(ns cljscrape.core
  (:require [cljscrape.scraper :as scraper]
            [cheshire.core :as json]
            [cljscrape.youtube :as yt])
  (:gen-class))

(defn -main
  [& args]
  (println "started now")
  (doall
   (pmap
    (fn [i id]
      (as-> id v
        (do (printf "starting scraping %s\n" v) v)
        (scraper/scrape-channel v)
        (json/encode v)
        (spit (format "/tmp/%d.json" i) v)))
    (range) args))
  (println "done!")
  (System/exit 0))
