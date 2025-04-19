(ns cljscrape.core
  (:require [cljscrape.scraper :as scraper]
            [cljscrape.constants :as consts]
            [cljscrape.queue :as queue])
  (:import [java.sql DriverManager]
           [org.slf4j LoggerFactory])
  (:gen-class))

(def logger (LoggerFactory/getLogger "main"))

(defn -main [& args]
  (.info logger "started now")
  (scraper/scrape-loop
   consts/initial-fail-sleep-time
   (queue/->PostgresQueue
    (DriverManager/getConnection consts/db-url consts/db-user consts/db-pass)))
  (.info logger "done!")
  (System/exit 0))
