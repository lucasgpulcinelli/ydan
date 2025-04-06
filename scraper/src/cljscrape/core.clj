(ns cljscrape.core
  (:require [cljscrape.scraper :as scraper]
            [cheshire.core :as json]
            [cljscrape.utils :as utils])
  (:gen-class))

(defn -main
  [& args]
  (println "started now")
  (doall
   (pmap
    (fn [i id]
      (as-> id v
        (scraper/scrape v)
        (if (= :video (utils/kind id))
          (let [f (java.io.File. (format "/tmp/%d.jpeg" i))
                is (java.io.FileOutputStream. f)]
            (.write is (get v "thumbnail"))
            (.close is)
            (spit (format "/tmp/%d.xml" i) (get v "captions"))
            (-> v (dissoc "thumbnail") (dissoc "captions")))
          v)
        (json/encode v)
        (spit (format "/tmp/%d.json" i) v)))
    (range) args))
  (println "done!")
  (System/exit 0))
