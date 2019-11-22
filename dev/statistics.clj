(ns statistics
  (:require [datomic.api :as d]))

(def schema
  [{:db/ident :query-optimizer.statistics/datom-count
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "Metadata installed by datomic-query-optimizer to track counts of attributes."}])

(defn schema-installed? [db]
  (seq (d/q '[:find [?e ...]
              :where
              [?e :db/ident :query-optimizer.statistics/datom-count]]
            db)))

(defn install-schema [conn]
  (let [db (d/db conn)]
    (when-not (schema-installed? db)
      @(d/transact conn schema))))

(defn all-attrs [db]
  (d/q '[:find [?e ...]
         :where
         [_ :db.install/attribute ?e]]
       db))

(defn eid->ident [db eid]
  (-> (d/pull db '[:db/ident] eid)
      (:db/ident)))

(defn schema-attr?
  "Sort of a heuristic, but covers known cases (and won't usually conflict)."
  [db eid]
  (when-let [ident (eid->ident db eid)]
   (let [ident-ns (namespace ident)]
    (or (= "db" ident-ns)
        (= "fressian" ident-ns)
        (.startsWith ^String ident-ns "db.")))))

(defn stats-attr?
  [db eid]
  (#{:query-optimizer.statistics/datom-count} (eid->ident db eid)))

(defn installed-attrs [db]
  (->> (all-attrs db)
       (remove (partial schema-attr? db))
       (remove (partial stats-attr? db))))

(defn attr->count [db attr]
  (count (seq (d/datoms db :aevt attr))))

(defn count-attributes [db]
  (let [attrs (installed-attrs db)]
    (zipmap (map (partial eid->ident db) attrs)
            (map (partial attr->count db) attrs))))

(defn create-tx-data [db]
  (for [[attr count-stat] (count-attributes db)]
    {:db/id attr :query-optimizer.statistics/datom-count count-stat}))

(defn update! [conn]
  (install-schema conn)
  (let [db (d/db conn)
        updates (create-tx-data db)]
    @(d/transact conn updates)))


(defn retrieve [db]
  (->> (d/datoms db :aevt :query-optimizer.statistics/datom-count)
       (map (fn [[e a v]]
              [(eid->ident db e) v]))
       (into {})))

(comment
  (update! conn))
