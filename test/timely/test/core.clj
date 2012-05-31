(ns timely.test.core
  (:use [timely.core])
  (:use [clojure.test])
  (:require [clj-time.core :as dates])
  (:require [clj-time.coerce :as dates-coerce]))

;; Demo and testing

(defn to-utc-timestamp
  "Convert from clj-time date to a timestamp in the utc timezone"
  [date]
  (dates-coerce/to-long
   (dates/from-time-zone date
                         (dates/default-time-zone))))

(defn test-print-schedule
  [test_id]
  (println (str (java.util.Date.) ": Item scheduled - " test_id)))

(defn test-print-fn
  [test_id]
  (partial test-print-schedule test_id))

(defn run-schedules-test-simple
  "This demo shows how to add/remove schedule items using
  start-schedule and end-schedule.  The schedule will be removed after
  2 minutes in this demo."
  []
  (start-scheduler)
  (let [item (scheduled-item
              (each-minute)
              (test-print-fn "Scheduled using start-schedule"))]
    (let [sched-id (start-schedule item)]
      (Thread/sleep (* 1000 60 2))
      (end-schedule sched-id)))
  (while true
    (Thread/sleep (* 1000 60))))

(deftest test-schedule-to-cron
  (is (= "0 0 * * *"     (schedule-to-cron (daily))))
  (is (= "20 9 * * *"    (schedule-to-cron (daily (at (hour 9) (minute 20))))))
  (is (= "0 0 * 4 *"     (schedule-to-cron (daily (on (month :apr))))))
  (is (= "20 9 * 4 *"    (schedule-to-cron (daily (at (hour 9) (minute 20)) (on (month :apr))))))
  (is (= "20 9 3 * *"    (schedule-to-cron (monthly (at (hour 9) (minute 20) (day 3))))))
  (is (= "0 * * 4-9 1"   (schedule-to-cron (hourly (on (day-of-week :mon) (month (in-range :apr :sep)))))))
  (is (= "0 * * 4-9 1,5" (schedule-to-cron (hourly (on (day-of-week :mon :fri) (month (in-range :apr :sep)))))))
  (is (= "0 0,12 * * *"  (schedule-to-cron (daily (at (hour 0 12))))))
  (is (= "0 0,12 * * *"  (schedule-to-cron (daily (at (hour (am 12) (pm 12)))))))
  (is (= "20 9 * * 1,3"  (schedule-to-cron (on-days-of-week [:mon :wed] (at (hour 9) (minute 20))))))
  (is (= "*/2 * * * *"   (schedule-to-cron (every 2 :minutes))))
  (is (= "*/2 * */2 * *" (schedule-to-cron (every 2 :minutes (each 2 :days)))))
  (is (= "1 2 3 4 *"     (schedule-to-cron (create-schedule 1 2 3 :apr all))))
  (is (= "* * * * *"     (schedule-to-cron (each-minute (start-time (* (dates-coerce/to-long (dates/now)) 2))))))
  (is (= "* * * * *"     (schedule-to-cron (each-minute (end-time 0)))))
  (is (= "* * * * *"     (schedule-to-cron (each-minute (start-time 0) (end-time (* (dates-coerce/to-long (dates/now)) 2))))))
  (is (= "* * * * *"     (schedule-to-cron (each-minute
                                            (start-time (to-utc-timestamp (dates/date-time 2012 5 15 11 42)))
                                            (end-time (to-utc-timestamp (dates/date-time 2012 5 15 11 43))))))))
