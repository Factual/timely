(defproject factual/timely "0.0.1"
  :main timely.core
  :description "Timely: A clojure library for defining schedules and running them as an alternative to cron"
  :dependencies [[clj-time "0.4.1"]
                 [org.clojure/clojure "1.4.0-alpha2"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojars.ghoseb/cron4j "2.2.1"]]
  :dev-dependencies [[lein-clojars "0.6.0"]])
