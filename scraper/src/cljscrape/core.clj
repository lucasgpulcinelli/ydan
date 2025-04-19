(ns cljscrape.core
  (:require [cljscrape.scraper :as scraper]
            [cljscrape.constants :as consts]
            [cljscrape.queue :as queue])
  (:import [java.sql DriverManager])
  (:gen-class))

(defn -main [& args]
  (println "started now")
  (scraper/scrape-loop
   consts/initial-fail-sleep-time
   (queue/->PostgresQueue
    (DriverManager/getConnection consts/db-url consts/db-user consts/db-pass)))
  (println "done!")
  (System/exit 0))
