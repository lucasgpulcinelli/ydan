(ns cljscrape.core
  (:require [cljscrape.scraper :as scraper])
  (:import [java.sql DriverManager])
  (:gen-class))

(defn -main
  [& args]
  (let [dburl "jdbc:postgresql://postgres:5432/"
        user "postgres"
        pass "postgres"]
    (println "started now")
    (scraper/scrape-loop 1000 (DriverManager/getConnection dburl user pass))
    (println "done!"))
  (System/exit 0))
