## About

Timely is useful for two main purposes:

1. A clojure DSL for easier definition of cron strings
2. A scheduling library using cron4j to execute a function according to a schedule timetable.

## Cron

To get a cron string, define a schedule and use schedule-to-cron:

	(schedule-to-cron (each-minute))

## Define Schedules

Schedule definitions supported by cron syntax are fully supported by the clojure DSL.  Additionally, specific start and end times can be defined to ensure a repeated schedule is only valid for a certain time frame.

Define a scheduled-item using a schedule and a function to be executed on the defined schedule. For example:

         ;; Daily at 12:00am
         (scheduled-item (daily)
                         (test-print-fn 1))

(daily) creates a schedule that runs each day at 12:00am.  (test-print-fn 1) returns a function that will print a message.  The combined scheduled-item will print the message each day at 12:00am.

The following are further examples of the dsl for defining schedules, copied from core.clj:

         ;; Each day at 9:20am
         (scheduled-item (daily
                          (at (hour 9) (minute 20)))
                         (test-print-fn 2))

         ;; Each day at 12:00am in april
         (scheduled-item (daily
                          (on (month 4)))
                         (test-print-fn 3))

         ;; Each day at 9:20am in april
         (scheduled-item (daily
                          (at (hour 9) (minute 20))
                          (on (month :april)))
                         (test-print-fn 4))

         ;; Monthly on the 3rd at 9:20am
         (scheduled-item (monthly
                          (at (hour 9) (minute 20) (day 3)))
                         (test-print-fn 5))

         ;; Between months 4-9 on Mondays, each hour
         (scheduled-item (hourly
                          (on (day-of-week :mon)
                              (month (in-range 4 9))))
                         (test-print-fn 6))

         ;; Between months 4-9 on Mondays and Fridays, each hour
         (scheduled-item (hourly
                          (on (day-of-week :mon :fri)
                              (month (in-range :april :september))))
                         (test-print-fn 7))

         ;; On every 8am and 5pm
         (scheduled-item (daily
                          (at (hour 8 17)))
                         (test-print-fn 8))

         ;; On monday and wednesday at 9:20am
         (scheduled-item (on-days-of-week
                          [:mon :wed]
                          (at (hour 9) (minute 20)))
                         (test-print-fn 9))

         ;; Every 2 minutes
         (scheduled-item (every :minute
                                2)
                         (test-print-fn 10))

         ;; Every 2 minutes, but only every 2 days
         (scheduled-item (every :minute
                                2
                                (per :day 2))
                         (test-print-fn 11))

         ;; Every 2:01am on April 3rd
         (scheduled-item (create-schedule
                          1 2 3 4 all)
                         (test-print-fn 12))

         ;; Start time in the future
         (scheduled-item (each-minute
                          (start-time (*  (dates-coerce/to-long (dates/now)) 2)))
                         (test-print-fn 13))

         ;; End time already passed
         (scheduled-item (each-minute
                          (end-time 0))
                         (test-print-fn 14))

         ;; Is within range
         (scheduled-item (each-minute
                          (start-time 0)
                          (end-time (* (dates-coerce/to-long (dates/now)) 2)))
                         (test-print-fn 15))

         ;; Schedule within a specific time range
         (scheduled-item
          "specific-time-range"
          (each-minute
           (start-time (to-utc-timestamp (dates/date-time 2012 5 15 11 42)))
           (end-time (to-utc-timestamp (dates/date-time 2012 5 15 11 43))))
          (test-print-fn "specific-time-range"))
          
          
## Run Schedules

Use start-schedule and end-schedule to start and stop schedules in your application:

	(let [item (scheduled-item (each-minute)
    	                         (test-print-fn "Scheduled using start-schedule"))
       	  sched-id (:_id item)]
    	(start-schedule item)
    	(Thread/sleep (* 1000 60 2))
    	(end-schedule sched-id)
		(while true
			(Thread/sleep (* 1000 60))))

Use refresh-schedules to repeatedly check for additions/updates/removals from a list of existing schedules:

    (while true
      ;; Use refresh-schedules to constantly check for additions/updates/removals from existing schedules already added
      (refresh-schedules schedules)
      (Thread/sleep 2500))