(ns timely.core
  (:require [clj-time.core :as dates])
  (:require [clj-time.coerce :as dates-coerce])
  (:require [clojure.string :only (join)])
  (:import [it.sauronsoftware.cron4j Scheduler])
  (:use [clojure.tools.logging :only (info debug error)]))

;; [LEO] I don't know what the right convention is in Clojure, but should this be :all instead of "all"?
(def all
  "all")

(defn to-day-of-week
  ;; [LEO] actually, if day of week is "all", you don't return a number, right?
  "Convert a named day of the week to a number representation"
  [day-of-week]
  (if (= all day-of-week)
    day-of-week
    (if (instance? Long day-of-week)
      ;; [LEO] same as with letting people index months directly, this seems like it could be error prone.
      ;; Some might assume Sunday is 0, some might assume it's 1, some (non-US developers) might assume
      ;; Monday is 0 and Sunday is 6, etc)
      (if (and (>= day-of-week 0)
               (<= day-of-week 6))
        day-of-week
        (throw ( Exception. (str "Day of week is out of accepted range: " day-of-week))))
      (condp = day-of-week
        ;; [LEO] OCD comment: how about Tue and Thu so that all day of week keywords are 3 letters?
        :sun 0
        :mon 1
        :tues 2
        :wed 3
        :thurs 4
        :fri 5
        :sat 6
        (throw ( Exception. (str "Not a valid day of the week: " day-of-week)))))))

(defn to-month
  ;; [LEO] if month is "all", you don't return a number, right? I'm going to stop making this comment, but it
  ;; applies to to-minute, etc. as well.
  "Convert a named month to a number representation"
  [month]
  (if (= all month)
    month
    (if (instance? Long month)
      (if (and (>= month 1)  ;; [LEO] I commented on this in the README, but 1-12 is different from java (which is 0-11)
               (<= month 12))
        month
        (throw ( Exception. (str "Month is out of accepted range: " month)))) ;; [LEO] would be good to return expected range, too
      (condp = month
        ;; [LEO] OCD comment: how about jan/feb/mar/apr/etc. so that all month keywords are 3 letters?
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
        (throw ( Exception. (str "Not a valid month: " month)))))))

(defn to-minute
  "Convert to a valid minute number represenatation"
  [minute]
  (if (= all minute)
    minute
    (if (instance? Long minute)
      (if (and (>= minute 0)
               (<= minute 60)) ;; [LEO] should 60 be a supported minute? It seems like you should either support 0-59,
                               ;; or you should support anything >=0 (e.g. "every 75 minutes")
        minute
        (throw ( Exception. (str "Minute is out of accepted range: " minute))));; [LEO] would be good to return expected range, too
      (throw ( Exception. (str "Not a valid minute: " minute))))))

(defn to-hour
  "Convert to a valid hour number represenatation"
  [hour]
  (if (= all hour)
    hour
    (if (instance? Long hour)
      (if (and (>= hour 0)
               (<= hour 24)) ;; [LEO] same comment as in to-minute... either allow 0-59, or 0+
        hour
        (throw ( Exception. (str "Hour is out of accepted range: " hour))));; [LEO] would be good to return expected range, too
      (throw ( Exception. (str "Not a valid hour: " hour))))))

(defn to-day
  "Convert to a valid day number represenatation"
  [day]
  (if (= all day)
    day
    (if (instance? Long day)
      (if (and (>= day 0)
               (<= day 31)) ;; [LEO] same comment as in to-minute... either allow 0-30 or 0+ (or 1-31 or 1+ depending on indexing)
        day
        (throw ( Exception. (str "Day is out of accepted range: " day))));; [LEO] would be good to return expected range, too
      (throw ( Exception. (str "Not a valid day: " day))))))

(defn to-date-number
  "Convert a named date field value of type \"type\" to a number
  representation"
  [type value]
  (if (map? value)
    (reduce #(assoc %1 (first %2) (to-date-number type (second %2))) {} value)
    (condp = type
        :day-of-week (to-day-of-week value)
        :month (to-month value)
        :minute (to-minute value)
        :hour (to-hour value)
        :day (to-day value)
        value)))

(defn create-schedule
  "Create a schedule representation based on parameters.  Apply
  filters: at, on, per, start-time, end-time"
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
  "Create a schedule that runs every hour.  Optionally specify
   parameters using (at ...) to set the minute value at which this
   will run.  For example: (hourly (at (minute 10))) runs at the 10th
   minute after each hour.  If not specified, a default minute of 0 is
   used.  Filters available: at, on, per, start-time, end-time"
  [& filters]
  (apply (partial create-schedule 0 all all all all) filters))

(defn daily
  "Create a schedule that runs once each day.  Optionally specify
   parameters using (at ...) to set the hour and minute values at
   which this will run.  For example: (daily (at (hour 9) (minute
   30))) runs at 9:30am.  If not specified, a default hour and minute
   of 0 is used.  Filters available: at, on, per, start-time,
   end-time"
  [& filters]
  (apply (partial create-schedule 0 0 all all all) filters))

(defn weekly
  "Create a schedule that runs once each week.  Optionally specify
   parameters using (on ...) to set the specific day of the week on
   which it will be run, as well as (at ...) to set the specific time
   at which this will run.  For example: (weekly (on :wed) (at (hour
   9) (minute 10))) runs weekly on Wednesday at 9:10am.  If not
   specified, a default day of Sunday and a default hour and minute of
   0 is used.  Filters available: at, on, per, start-time, end-time"
   [& filters]
  (apply (partial create-schedule 0 0 all all 0) filters))

(defn monthly
  "Create a schedule that runs once every month.  Optionally specify
   parameters using (on ...) or (at ...) to set the day, hour, and
   minute values at which this will run.  For
   example: (monthly (on (day 3)) (at (hour 9) (minute 10))) runs on
   the 3rd at 9:10am on each month.  If not specified, a default hour
   and minute of 0 is used, and a default day of the 1st is used.
   Apply additional filters: at, on, per, start-time, end-time"
  [& filters]
  (apply (partial create-schedule 0 0 1 all all) filters))

(defn set-schedule-values
  "Sets schedule values as a list, converting to number
  representations"
  [sched type values]
  (assoc sched type (map #(to-date-number type %) values)))

(defn on-days
  "Create a schedule to run on each day of values in the \"value\"
  parameter, which is a list.  For example, (on-days [1 15]) will run
  on every 1st and 15th of the month.  Apply filters as needed."
  [value & filters]
  (set-schedule-values (apply daily filters) :day value))

(defn on-months
  "Create a schedule to run on each month of values in the \"value\"
  parameter, which is a list.  For example, (on-months [6 12]) will
  run on every June and December.  Apply filters as needed."
  [value & filters]
  (set-schedule-values (apply monthly filters) :month value))

(defn on-days-of-week
  "Create a schedule to run on each day of the week in the \"value\"
  parameter, which is a list.  For example, (on-days-of-week
  [:mon :fri]) will run on every Monday and Friday.  Apply filters as
  needed."
  [value & filters]
  (set-schedule-values (apply daily filters) :day-of-week value))

(defn create-interval
  "Store a time interval value"
  [interval]
  {:interval interval})

(defn per
  "Returns a filter for a schedule to run on a recurring interval.
  Specify a type and a value.  For example: (per :day 2) will define a
  filter for a schedule to only run every other day.  Note that this
  returns a filter and not a schedule."
  [type interval]
  ;; [LEO] any time you let the user set the type, you should validate the type. E.g. if someone sends :hr instead of
  ;; :hour, this code will "work" but the scheduling won't work as expected.
  {type (create-interval interval)})

(defn every
  "Returns a schedule which will be run on a recurring interval.
  Specify a type and value.  For example: (every :minute 5) will
  create a schedule which runs every 5 minutes."
  [type interval &
  filters]
  (merge (apply each-minute filters) (per type interval)))

(defn on
  "Returns a filter for a schedule to run on particular values for a
  date field of type \"type\".  For example: (on :weekdays :wed :fri)
  is a filter for a schedule to only run on Wednesday and Friday."
  [& values]
  (apply merge {} values))

(defn at
  "Returns a filter for a schedule to run at specific date field
  values.  For example: (at (hour 9) (minute 10)) specifies a run at
  9:10am."
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
  "Filter to specify a start time as a timestamp from which the schedule will start.
  It is inclusive, meaning a schedule set to run exactly at the start
  time will run at the start time."
  [start-time]
  (let [start-time (dates-coerce/from-long start-time)]
    {:start-time start-time}))

(defn end-time
  "Filter to specify an end time as a timestamp when the schedule will
  no longer run.  It is exclusive, meaning a schedule set to run
  exactly at the end time will not run at the end time."
  [end-time]
  (let [end-time (dates-coerce/from-long end-time)]
    {:end-time end-time}))

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
   (instance? Long item) item
   :else (throw ( Exception. (str "Error in converting to cron: " item)))))

(defn schedule-to-cron
  "Create a cron string from a schedule"
  [sched]
  (clojure.string/join " " (map to-cron-entry [(sched :minute) (sched :hour) (sched :day) (sched :month) (sched :day-of-week)])))

(defn scheduled-item
  "Create a scheduled item using a schedule and a function to execute
on intervals defined in the schedule."
  ([schedule work]
     {:schedule schedule
      :work work}))

;; Single scheduler for Timely
(def SCHEDULER (Scheduler.))

(defn end-schedule
  "Removes a schedule from Timely by descheduling based on a schedule
  id.  The schedule id is a unique identifier that was generated upon
  starting a schedule."
  [sched-id]
  (info "Ending schedule:" sched-id)
  (.deschedule SCHEDULER sched-id))

(defn process-scheduled-item
  "Executes work for a scheduled item, but only if within optionally
  specified start and end times."
  [work start-time end-time]
  (if (and
       end-time
       (dates/before? end-time
                      (dates/now)))
    (info "Schedule is no longer valid")
    (if (or (nil? start-time)
            (dates/before? start-time (dates/now)))
      (work)
      (info "Waiting to start a schedule"))))

(defn begin-schedule
  "Begin a schedule, returning a unique id for the added schedule."
  [work cron start-time end-time]
  (.schedule SCHEDULER cron #(process-scheduled-item work start-time end-time)))

(defn start-schedule
  "Adds the specified schedule to the scheduler based on start/end
  time restrictions.  Returns a unique identifier for this schedule
  that can be used to later deschedule."
  [{:keys [schedule work]}]
  (let [start_time (:start-time schedule)
        end_time (:end-time schedule)
        cron (schedule-to-cron schedule)
        now (dates/now)]
    (if (and end_time
             (dates/before? end_time now))
      (info "End date is before current time, not scheduling:" cron)
      (do
        (info "Starting schedule:" cron)
        (begin-schedule work cron start_time end_time)))))

(defn start-scheduler
  []
  (.start SCHEDULER))

;; Demo and testing
;; [LEO] should this be here, or in a separate test/sample file?
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