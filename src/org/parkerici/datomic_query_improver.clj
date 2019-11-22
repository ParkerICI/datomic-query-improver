(ns org.parkerici.datomic-query-improver
  (:require [clojure.set :as set]))

(defn- ->map-form
  "Converts list form Datomic query into map form."
  [q-edn]
  (->> (partition-by #{:find :in :where :with} q-edn)
       (partition-all 2)
       (map (fn [[k v]]
              [(first k) (vec v)]))
       (into {})))


(defn- clause->vars
  "For any clause in the :where portion of a datomic query,
  return any datalog vars that occur in that clause."
  [clause]
  (->> clause
       (filter symbol?)
       (filter (fn [sym]
                 (let [n (name sym)]
                   (.startsWith ^String n "?"))))))

(defn- clause->attr
  "When clause contains a keyword literal attr, returns it. Otherwise
  returns nil."
  [[_ a]]
  (when (keyword? a)
    a))

(defn db-stats->attr-counts
  "Given database stats as returned by datomic client:
  `datomic-client-api/db-stats`, returns attr-counts map in form that
  `suggest` arg accepts."
  [{:keys [attrs]}]
  (->> (for [[a {:keys [count]}] attrs]
         [a count])
       (into {})))


(defn suggest
  "Given dictionary of attribute counts (as per stats/retrieve or derived from
  db-stats) and list or map form query, returns a map form query in which
  :where clauses have been (possibly) re-ordered into a more efficient ordering.
  Uses two heuristics: join along, and most restrictive clauses first."
  [attr-counts q-edn]
  (let [q (if (map? q-edn)
            q-edn
            (->map-form q-edn))
        {:keys [where in]} q]
    (loop [bound (if in (set (clause->vars in)) #{})
           where-clauses where
           out-ordering []]
      (if-not (seq where-clauses)
        (assoc q :where out-ordering)
        (let [by-vars (mapv clause->vars where-clauses)
              new-bindings (map #(set/difference (set %) bound) by-vars)
              bind-counts (map count new-bindings)
              ;; Note: defaults to counting "0" for any attributes not included in stats
              approx-datom-counts (map #(get attr-counts % 0) (map clause->attr where-clauses))
              clause->score (->> (map vector bind-counts approx-datom-counts)
                                 (zipmap where-clauses))
              best-bound (->> clause->score
                              (sort-by (fn [[_ binds datoms]]
                                         [binds datoms]))
                              (ffirst))
              vars-bound (clause->vars best-bound)]
          (recur (apply conj bound vars-bound)
                 (disj (set where-clauses) best-bound)
                 (conj out-ordering best-bound)))))))
