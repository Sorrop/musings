(ns musings.compute-test
  (:require [ubergraph.alg :as alg]
            [musings.compute :as compute]
            [clojure.math.combinatorics :as comb]
            [clojure.test :as t]))


(defn task-fn [id]
  (fn []
    (let [started (java.util.Date.)
          sleep (rand-int 10000)]
      (println (format "Executing %s" id))
      (Thread/sleep sleep)
      {:result      id
       :sleep       sleep
       :started-at  started
       :finished-at (java.util.Date.)})))

(defn construct-tasks [n]
  (->> (for [id (repeatedly n random-uuid)]
         [id {:f (task-fn id)}])
       (into (hash-map))))

(defn construct-execution [n]
  (let [tasks    (construct-tasks n)
        edges    (->> (comb/combinations (keys tasks) 2)
                      (random-sample 0.15)
                      (map vec)
                      (into []))
        node-ids (distinct (mapcat identity edges))
        nodes    (for [id node-ids]
                   [id
                    {:id    id
                     :state :pending}])
        graph    (compute/->dag nodes edges)]
    (if (and (seq nodes)
             (alg/dag? graph))
      {:tasks (select-keys tasks node-ids)
       :graph graph}
      (construct-execution n))))

(defn prepare-db [n-dags k-nodes]
  (reduce (fn [acc {:keys [tasks graph]}]
            (-> acc
                (update :tasks merge tasks)
                (update :executions assoc (random-uuid) graph)))
          {:tasks      {}
           :executions {}}
          (repeatedly n-dags #(construct-execution k-nodes))))

(def db
  (atom (prepare-db 8 10)))

(def executions
  (atom (->> (keys (:executions @db))
             (map #(hash-map :id %))
             (into []))))

(defn wait-until-finished [db]
  (let [graphs (vals (:executions @db))
        all-finished? (every? compute/dag-finished? graphs)]
    (if (not all-finished?)
      (do
        (Thread/sleep 2000)
        (wait-until-finished db))
      :ok)))

(defn do-test [db executions n-threads]
  (let [threads (reduce
                 (fn [acc _]
                   (let [f (future
                             (compute/worker {:executions executions
                                              :db         db}))]
                     (conj acc f)))
                 []
                 (range n-threads))]
    (wait-until-finished db)
    (println "Run Finished!!!")
    (doseq [t threads]
      (future-cancel t))))

(comment

  (def dag-1
    (compute/->dag [[:a {:id    :a
                         :state :pending}]
                    [:b {:id    :b
                         :state :pending}]
                    [:c {:id    :c
                         :state :pending}]
                    [:d {:id    :d
                         :state :pending}]]
                   [[:a :b]
                    [:a :c]
                    [:c :d]
                    [:b :d]]))

  (def dag-2
    (compute/->dag [[:e {:id    :e
                         :state :pending}]
                    [:f {:id    :f
                         :state :pending}]
                    [:g {:id    :g
                         :state :pending}]
                    [:h {:id    :h
                         :state :pending}]
                    [:i {:id    :i
                         :state :pending}]]
                   [[:e :f]
                    [:e :g]
                    [:g :h]
                    [:f :h]]))

  {:tasks
   {:a {:f (task-fn :a)}
    :b {:f (task-fn :b)}
    :c {:f (task-fn :c)}
    :d {:f (task-fn :d)}
    :e {:f (task-fn :e)}
    :f {:f (task-fn :f)}
    :g {:f (task-fn :g)}
    :h {:f (task-fn :h)}
    :i {:f (task-fn :i)}}
   :executions
   {1 dag-1
    2 dag-2}})
