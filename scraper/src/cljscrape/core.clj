(ns cljscrape.core
  (:require [cljscrape.scraper :as scraper])
  (:gen-class))

(defn -main
  [& args]
  (println "started now")
  (scraper/scrape-loop 1000 (map scraper/make-entry args))
  (println "done!")
  (System/exit 0))
