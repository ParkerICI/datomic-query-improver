(ns user
  (:require [datomic.api :as d]
            [org.parkerici.datomic-query-improver :as improve]
            [statistics :as stats]))


(comment
  ;;db-stats helper
  (require '[clojure.java.io :as io])
  (require '[clojure.edn :as edn])
  (def db-stats
    (-> (slurp "dev/resources/client-db-stats.edn")
        (edn/read-string)))
  (improve/db-stats->attr-counts db-stats))

