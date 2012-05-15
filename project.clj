(defproject timely "1.0.0"
  :main timely.core
  :description "Timely: A clojure library for defining schedules and running them as an alternative to cron"
  :dependencies [[clj-time "0.4.1"]
                 [org.clojure/clojure "1.4.0-alpha2"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [org.clojars.ghoseb/cron4j "2.2.1"]])