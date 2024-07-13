(ns musings.dag
  (:require [musings.resource-pool :as p]
            [ubergraph.core :as uber]
            [ubergraph.alg :as alg]))


(defn ->dag [nodes edges]
  (-> (uber/ubergraph false false)
      (uber/add-nodes-with-attrs* nodes)
      (uber/add-edges* edges)))

(comment
  ;; node
  {:id "id"
   :state [:pending :running :completed :failed :skipped]
   :f (fn [])
   }
  )

(defn nodes->attrs [g nodes]
  (map #(uber/attrs g %) nodes))

(defn pending-nodes [g]
  (->> (alg/topsort g)
       (nodes->attrs g)
       (filterv #(= (:state %) :pending))))

(defn nodes-by-state [g]
  (->> (uber/nodes g)
       (nodes->attrs g)
       (group-by :state)))

(defn running-nodes [g]
  (->> (uber/nodes g)
       (nodes->attrs g)
       (filterv #(= (:state %) :running))))

(defn get-predecessors [g n]
  (mapv #(uber/attrs g %) (uber/predecessors* g n)))

(defn all-dependents [g n]
  (loop [[x & xs] (uber/successors g n)
         result []]
    (if (nil? x)
      result
      (let [xs (->> (uber/successors g x)
                    (concat xs)
                    distinct)]
        (recur xs (conj result x))))))

(defn satisfied? [nodes]
  (or (empty? nodes)
      (every? #(= (:state %) :completed) nodes)))

(defn pick [g pending]
  (loop [[n & ns] pending]
    (when (some? n)
      (let [dependencies (get-predecessors g (:id n))]
        (if (satisfied? dependencies)
          n
          (recur ns))))))

(defn next-task [graph]
  (let [pending (pending-nodes graph)]
    (when-not (empty? pending)
      (pick graph pending))))

(defn mark-downstream-failures [{:keys [id] :as task} g]
  (let [dependents (all-dependents g id)
        full-nodes (nodes->attrs g dependents)]
    (println full-nodes)
    (reduce (fn [acc {:keys [id] :as node}]
              (uber/set-attrs acc id (assoc node :state :skipped)))
            g
            full-nodes)))

(defn execute [state task]
  (let [{:keys [f id]} task
        result (try
                 {:state :completed
                  :result (f)}
                 (catch Throwable t
                   {:state :failed
                    :error  t}))
        g (p/acquire-resource state)]
    (if (= (:state result) :failed)
      (->> (uber/set-attrs g id (merge task result))
           (mark-downstream-failures task)
           (p/release-resource state))
      (->> (uber/set-attrs g id (merge task result))
           (p/release-resource state)))))

(defn finished? [g]
  (let [{:keys [pending running]} (nodes-by-state g)]
    (= 0
       (count pending)
       (count running))))

(defn worker
  [{:keys [state sleep results]
    :or   {sleep 1000}}]
  (loop []
    (try
      (when-let [g (p/try-acquire state)]
        (let [task (next-task g)
              id   (:id task)]
          (cond
            (some? task)
            (do
              (p/release-resource state (uber/set-attrs g id (assoc task :state :running)))
              (execute state task))

            (finished? g)
            (p/release-resource state g)
            #_(uber/remove-all g)

            :else
            (p/release-resource state g))))
      (Thread/sleep sleep)
      (catch Exception e
        (println e)))
    (recur)))

(def dag
  (->dag [[:a {:id    :a
               :f     #(do (println "Executing a")
                           (Thread/sleep 6000)
                           {:a 1})
               :state :pending}]
          [:b {:id    :b
               :f     #(do (println "Executing b")
                           (Thread/sleep 3000)
                           {:b 2})
               :state :pending}]
          [:c {:id    :c
               :f     #(do (println "Executing c")
                           (/ 0)
                           (Thread/sleep 4000)
                           {:c 3})
               :state :pending}]
          [:d {:id    :d
               :f     #(do (println "Executing d")
                           (Thread/sleep 2000)
                           {:d 4})
               :state :pending}]]
         [[:a :b]
          [:a :c]
          [:c :d]
          [:b :d]]))

(def state
  (let [create-graph (fn []
                       dag)]
    (p/resource-pool 1 create-graph)))


(comment
  (dotimes [_ 2]
    (future (worker {:state state}))))
