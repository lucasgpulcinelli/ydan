(defproject cljscrape "0.0.1"
  :description "A youtube scraper"
  :url "https://github.com/lucasgpulcinelli/cljscrape"
  :license {:name "BSD-3-Clause"
            :url "https://opensource.org/license/BSD-3-clause"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.postgresql/postgresql "42.7.5"]
                 [cheshire "5.13.0"]
                 [ch.qos.logback/logback-classic "1.5.18"]
                 [software.amazon.awssdk/s3 "2.31.23"]]
  :main ^:skip-aot cljscrape.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})

