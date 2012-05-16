(ns timely.core
  (:require [clj-time.core :as dates])
  (:require [clj-time.coerce :as dates-coerce])
  (:use [clojure.contrib.command-line :only (with-command-line)])
  (:use [clojure.pprint :only (pprint)])
  (:require [clojure.string :as clojure.string])
  (:import [it.sauronsoftware.cron4j Scheduler]))

(def all
  "all")

(defn to-day-of-week
  "Convert a named day of the week to a number representation"
  [day-of-week]
  (condp = day-of-week
                :sun 0
                :mon 1
                :tues 2
                :wed 3
                :thurs 4
                :fri 5
                :sat 6
                day-of-week))

(defn to-month
  "Convert a named month to a number representation"
  [month]
  (condp = month
                :january 1
                :february 2
                :march 3
                :april 4
                :may 5
                :june 6
                :july 7
                :august 8
                :september 9
                :october 10
                :november 11
                :december 12
                month))

(defn to-date-number
  "Convert a named date field value of type \"type\" to a number representation"
  [type value]
  (if (map? value)
    (reduce #(assoc %1 (first %2) (to-date-number type (second %2))) {} value)
    (condp = type
        :day-of-week (to-day-of-week value)
        :month (to-month value)
        value)))

(defn create-schedule
  "Create a schedule representation based on parameters.
   Apply filters: at, on, per, start-time, end-time"
  [minute hour day month day-of-week & filters]
  (apply merge
         {:minute (to-date-number :minute minute)
          :hour (to-date-number :hour hour)
          :day (to-date-number :day day)
          :month (to-date-number :month month)
          :day-of-week (to-date-number :day-of-week day-of-week)}
         filters))

(defn each-minute
  "Create a schedule that runs every minute.
   Filters available: on, per, start-time, end-time"
  [& filters]
  (apply (partial create-schedule all all all all all) filters))

(defn hourly
  "Create a schedule that runs every hour.
   Optionally specify parameters using (at ...) to set the minute value at which this will run.
   For example: (hourly (at (minute 10))) runs at the 10th minute after each hour.  If not specified, a default minute of 0 is used.
   Filters available: at, on, per, start-time, end-time"
  [& filters]
  (apply (partial create-schedule 0 all all all all) filters))

(defn daily
  "Create a schedule that runs once each day.
   Optionally specify parameters using (at ...) to set the hour and minute values at which this will run.
   For example: (daily (at (hour 9) (minute 30))) runs at 9:30am.  If not specified, a default hour and minute of 0 is used.
   Filters available: at, on, per, start-time, end-time"
  [& filters]
  (apply (partial create-schedule 0 0 all all all) filters))

(defn weekly
  "Create a schedule that runs once each week.
   Optionally specify parameters using (on ...) to set the specific day of the week on which it will be run, as well as (at ...) to set the specific time at which this will run.
   For example: (weekly (on :wed) (at (hour 9) (minute 10))) runs weekly on Wednesday at 9:10am.  If not specified, a default day of Sunday and a default hour and minute of 0 is used.
   Filters available: at, on, per, start-time, end-time"
  [& filters]
  (apply (partial create-schedule 0 0 all all 0) filters))

(defn monthly
  "Create a schedule that runs once every month.
   Optionally specify parameters using (on ...) or (at ...) to set the day, hour, and minute values at which this will run.
   For example: (monthly (at (day 3) (hour 9) (minute 10))) runs on the 3rd at 9:10am on each month.  If not specified, a default hour and minute of 0 is used, and a default day of the 1st is used.
   Apply additional filters: at, on, per, start-time, end-time"
  [& filters]
  (apply (partial create-schedule 0 0 1 all all) filters))

(defn set-schedule-values
  "Sets schedule values as a list, converting to number representations"
  [sched type values]
  (assoc sched type (map #(to-date-number type %) values)))

(defn on-days
  "Create a schedule to run on each day of values in the \"value\" parameter, which is a list.
   For example, (on-days [1 15]) will run on every 1st and 15th of the month.  Apply filters as needed."
  [value & filters]
  (set-schedule-values (apply daily filters) :day value))

(defn on-months
  "Create a schedule to run on each month of values in the \"value\" parameter, which is a list.
   For example, (on-months [6 12]) will run on every June and December.  Apply filters as needed."
  [value & filters]
  (set-schedule-values (apply monthly filters) :month value))

(defn on-days-of-week
  "Create a schedule to run on each day of the week in the \"value\" parameter, which is a list.
   For example, (on-days-of-week [:mon :fri]) will run on every Monday and Friday.  Apply filters as needed."
  [value & filters]
  (set-schedule-values (apply daily filters) :day-of-week value))

(defn create-interval
  "Store a time interval value"
  [interval]
  {:interval interval})

(defn per
  "Returns a filter for a schedule to run on a recurring interval.  Specify a type and a value.
   For example: (per :day 2) will define a filter for a schedule to only run every other day.
   Note that this returns a filter and not a schedule."
  [type interval]
  {type (create-interval interval)})

(defn every
  "Returns a schedule which will be run on a recurring interval.  Specify a type and value.
   For example: (every :minute 5) will create a schedule which runs every 5 minutes."
  [type interval & filters]
  (merge (apply each-minute filters) (per type interval)))

(defn on
  "Returns a filter for a schedule to run on particular values for a date field of type \"type\".
   For example: (on :weekdays :wed :fri) is a filter for a schedule to only run on Wednesday and Friday."
  [& values]
  (apply merge {} values))

(defn at
  "Returns a filter for a schedule to run at specific date field values.
   For example: (at (hour 9) (minute 10)) specifies a run at 9:10am."
  [& values]
  (apply merge {} values))

(defn in-range
  "Specify a range for a particular date field of type \"type\"."
  [start end]
  {:start start :end end})

(defn hour
  "Hour representation"
  [& hour]
  {:hour (map #(to-date-number :hour %) hour)})

(defn minute
  "Minute representation."
  [& minute]
  {:minute (map #(to-date-number :minute %) minute)})

(defn day
  "Day representation"
  [& day]
  {:day (map #(to-date-number :day %) day)})

(defn month
  "Month representation"
  [& month]
     {:month (map #(to-date-number :month %) month)})

(defn day-of-week
  "Day of week representation"
  [& day-of-week]
  {:day-of-week (map #(to-date-number :day-of-week %) day-of-week)})

(defn start-time
  "Filter to specify a start time from which the schedule will start.  It is inclusive, meaning a schedule set to run exactly at the start time will run at the start time."
  [start-time]
  {:start-time start-time})

(defn end-time
  "Filter to specify an end time when the schedule will not longer run.  It is exclusive, meaning a schedule set to run exactly at the end time will not run at the end time."
  [end-time]
  {:end-time end-time})

(defn time-to-cron
  "Convert a timestamp to a cron string in the current time zone"
  [timestamp]
  (let [date (dates/to-time-zone (dates-coerce/from-long timestamp)
                                 (dates/default-time-zone))]
    (clojure.string/join " "
                         [ (dates/minute date)
                           (dates/hour date)
                           (dates/day date)
                           (dates/month date)
                           (dates/day-of-week date)])))

(defn to-cron-entry
  "Convert a schedule date field value representation to a cron entry"
  [item]
  (cond
   (= all item) "*"
   (or (seq? item) (vector? item)) (clojure.string/join "," (map to-cron-entry item))
   (map? item) (if-let [interval (:interval item)]
                 (str "*/" interval)
                 (str (:start item) "-" (:end item)))
   :else item))

(defn schedule-to-cron
  "Create a cron string from a schedule"
  [sched]
  (clojure.string/join " " (map to-cron-entry [(sched :minute) (sched :hour) (sched :day) (sched :month) (sched :day-of-week)])))

(defn scheduled-item
  "Create a scheduled item using a schedule and a function to execute on intervals defined in the schedule.
   Optionally include a custom schedule id in order to later remove and update schedules in a running instance."
  ([sched-id schedule work]
     {:_id sched-id
      :schedule schedule
      :work work})
  ([schedule work]
     (scheduled-item (str (java.util.UUID/randomUUID)) schedule work)))

(defn to-utc-timestamp
  "Convert from clj-time date to a timestamp in the utc timezone"
  [date]
  (dates-coerce/to-long
   (dates/from-time-zone date
                         (dates/default-time-zone))))

;; Single scheduler for Timely
(def SCHEDULER (Scheduler.))

;; Keep track of all schedules currently known to Timely.
;; Mapping between schedule id and schedule entries
(def CURRENT_SCHEDULES {})

(defn schedule-entry
  "Create a schedule entry to keep track of existing schedules known to Timely."
  [insert_time cron-id]
  {:insert_time insert_time
   :cron-id cron-id})

(defn end-schedule
  "Removes a schedule from Timely by descheduling"
  [sched-id]
  (if-let [schedule-entry (CURRENT_SCHEDULES sched-id)]
    (do
      (if-let [cron-id (:cron-id schedule-entry)]
          (do
            (println "Removing schedule:" sched-id)
            (.deschedule SCHEDULER cron-id)))
        (def CURRENT_SCHEDULES (dissoc CURRENT_SCHEDULES sched-id)))))


(defn process-scheduled-item
  "Executes work for a scheduled item, but only if within optionally specified start and end times."
  [sched-id work start-time end-time]
  (if (and
       (not (nil? end-time))
       (dates/before? (dates-coerce/from-long end-time)
                      (dates/now)))
    (end-schedule sched-id)
    (if (or (nil? start-time)
            (dates/before? (dates-coerce/from-long start-time) (dates/now)))
      (work)
      (println "Waiting to start a schedule:" sched-id))))

(defn begin-schedule
  "Begin a schedule."
  [_id work cron insert_time start-time end-time]
  (let [cron-id (.schedule SCHEDULER cron #(process-scheduled-item _id work start-time end-time))]
    (def CURRENT_SCHEDULES
      (assoc CURRENT_SCHEDULES
        _id
        (schedule-entry insert_time cron-id)))))

(defn start-schedule
  "Adds the specified schedule to the scheduler based on start/end time restrictions."
  [{:keys [_id schedule work insert_time]}]
  (let [start_time (:start-time schedule)
        end_time (:end-time schedule)
        cron (schedule-to-cron schedule)
        now (dates/now)]
    (def CURRENT_SCHEDULES (assoc CURRENT_SCHEDULES _id {:insert_time insert_time}))
    (if (and (not (nil? end_time))
             (dates/before? (dates-coerce/from-long end_time) now))
      (println "End date is before current time, not scheduling:" _id "-" cron)
      (do
        (println "Starting schedule:" _id "-" cron)
        (begin-schedule _id work cron insert_time start_time end_time)))))

(defn process-schedule
  "Determine if a schedule need to be added and add if necessary.
   If a schedule needs to be updated based on a changed :insert_time, reload the schedule"
  [sched existing-schedules]
  (let [{:keys [_id insert_time]} sched]
    (if (contains? existing-schedules _id)
      (when-not (= insert_time
                   (:insert_time (existing-schedules _id)))
        (println "Updating schedule that changed:" _id)
        (end-schedule _id)
        (start-schedule sched))
      (do
        (start-schedule sched)))))

(defn deschedule-missing
  "Remove a schedule that is now missing from the existing-ids."
  [sched-id existing-ids]
  (when-not (contains? existing-ids sched-id)
    (end-schedule sched-id)))

(defn refresh-schedules
  "Update the complete set of schedules Timely is processing.  Any changes in the schedule list from a previous refresh will add/remove/update schedules from Timely based on diffs."
  [schedules]
  (let [existing-ids (reduce #(assoc %1 (:_id %2) %2) {} schedules)]
    ;; Add/update schedules that exist in the schedule list
    (dorun (map #(process-schedule % CURRENT_SCHEDULES) schedules))
    ;; Deschedule those which don't exist
    (dorun (map #(deschedule-missing % existing-ids) (map first CURRENT_SCHEDULES)))))

(defn start-scheduler
  []
  (.start SCHEDULER))

;; Demo and testing

(defn test-print-schedule
  [test_id]
  (println (str (java.util.Date.) ": Item scheduled - " test_id)))

(defn test-print-fn
  [test_id]
  (partial test-print-schedule test_id))

(defn run-schedules-test-simple
  "This demo shows how to add/remove schedule items using start-schedule and end-schedule.
   The schedule will be removed after 2 minutes in this demo."
  []
  (start-scheduler)
  (let [item (scheduled-item (each-minute)
                             (test-print-fn "Scheduled using start-schedule"))
        sched-id (:_id item)]
    (start-schedule item)
    (Thread/sleep (* 1000 60 2))
    (end-schedule sched-id)
    (while true
      (Thread/sleep (* 1000 60)))))
