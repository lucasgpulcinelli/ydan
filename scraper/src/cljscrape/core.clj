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
        (yt/id2url v)
        (scraper/scrape v)
        (let [f (java.io.File. (format "/tmp/%d.jpeg" i))
              is (java.io.FileOutputStream. f)]
          (.write is (get v "thumbnail"))
          (.close is)
          (dissoc v "thumbnail"))
        (do
          (spit (format "/tmp/%d.xml" i) (get v "captions"))
          (dissoc v "captions"))
        (json/encode v)
        (spit (format "/tmp/%d.json" i) v)))
    (range) args))
  (println "done!")
  (System/exit 0))
