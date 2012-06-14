## About

Timely is a Clojure library for scheduling tasks according to a timetable, as an in-process alternative to cron.  To make this easy, Timely provides a DSL with a more intuitive syntax for definition of cron-style schedules.  Furthermore, it also allows optional specification of a valid time window within which a schedule will be run.  Consider including Timely in your project for a simple, lightweight approach to execute any function on a repeated schedule.

## Installation

The library is hosted at Clojars. Just add this to your dependencies:

```
[factual/timely "0.0.2"]
```

## Setup

```
(ns yournamespace.core
  (:require [timely.core :as timely]))
  (timely/start-scheduler)
```

## Schedule DSL and Cron

Schedules are a structured way to represent cron syntax and are created using a DSL which reads much like an English sentence.  To get a cron string from a schedule, use schedule-to-cron.  For example:

	timely.core> (schedule-to-cron (each-minute))
	"* * * * *"
	
See the "Define Schedules" section below for more examples of the schedule DSL.

## Define Schedules

Define a scheduled-item using a schedule and a function to be executed on the defined schedule. For example:

```clojure
;; Daily at 12:00am
(scheduled-item (daily)
				(test-print-fn 1))
```

(daily) creates a schedule that runs each day at 12:00am.  (test-print-fn 1) returns a function that will print a message.  The combined scheduled-item will print the message each day at 12:00am.

Specific start and end times can be optionally defined to ensure a repeated schedule is only valid for a certain time frame.  This is a feature recognized by the Timely scheduler but does not exist in cron string syntax.

The following are further examples of the dsl for defining schedules:

```clojure
;; Each day at 9:20am
(scheduled-item (daily
                 (at (hour 9) (minute 20)))
                (test-print-fn 2))

;; Each day at 12:00am in april
(scheduled-item (daily
                 (on (month :apr)))
                (test-print-fn 3))

;; Each day at 9:20am in april
(scheduled-item (daily
                 (at (hour 9) (minute 20))
                 (on (month :apr)))
                (test-print-fn 4))

;; Monthly on the 3rd at 9:20am
(scheduled-item (monthly
                 (at (hour 9) (minute 20) (day 3)))
                (test-print-fn 5))

;; Between months 4-9 on Mondays, each hour
(scheduled-item (hourly
                 (on (day-of-week :mon)
                     (month (in-range :apr :sep))))
                (test-print-fn 6))

;; Between months 4-9 on Mondays and Fridays, each hour
(scheduled-item (hourly
                 (on (day-of-week :mon :fri)
                     (month (in-range :apr :sep))))
                (test-print-fn 7))

;; On every 12am and 12pm
(scheduled-item (daily
                 (at (hour 0 12)))
                (test-print-fn 8))

;; On every 12am and 12pm
(scheduled-item (daily
                 (at (hour (am 12) (pm 12))))
                (test-print-fn 8))

;; On monday and wednesday at 9:20am
(scheduled-item (on-days-of-week
                 [:mon :wed]
                 (at (hour 9) (minute 20)))
                (test-print-fn 9))

;; Every 2 minutes
(scheduled-item (every 2
                       :minutes)
                (test-print-fn 10))

;; Every 2 minutes, but only every 2 days
(scheduled-item (every 2
                       :minutes
                       (each 2 :days))
                (test-print-fn 11))

;; Every 2:01am on April 3rd
(scheduled-item (create-schedule
                 1 2 3 :apr :all)
                (test-print-fn 12))

;; Schedule only valid within a specific time range
(scheduled-item
 (each-minute
  (start-time 1337107320000)
  (end-time 1337107380000))
 (test-print-fn "specific-time-range"))
```     
          
## Run Schedules

Use (start-scheduler) to enable scheduling in your application.

Use start-schedule and end-schedule to start and stop schedules in your application:

```clojure
(start-scheduler)
(let [item (scheduled-item
            (each-minute)
            (test-print-fn "Scheduled using start-schedule"))]
  (let [sched-id (start-schedule item)]
    (Thread/sleep (* 1000 60 2))
    (end-schedule sched-id)))
```
