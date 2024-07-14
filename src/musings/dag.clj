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

(defn pending? [task]
  (= (:state task) :pending))

(defn completed? [task]
  (= (:state task) :completed))

(defn failed? [task]
  (= (:state task) :failed))

(defn nodes->attrs [g nodes]
  (map #(uber/attrs g %) nodes))

(defn pending-nodes [g]
  (->> (alg/topsort g)
       (nodes->attrs g)
       (filterv pending?)))

(defn nodes-by-state [g]
  (->> (uber/nodes g)
       (nodes->attrs g)
       (group-by :state)))

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
      (every? completed? nodes)))

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

(defn mark-downstream-failures [root-id g]
  (let [dependents (all-dependents g root-id)
        full-nodes (nodes->attrs g dependents)]
    (reduce (fn [acc {:keys [id] :as node}]
              (uber/set-attrs acc id (assoc node :state :skipped)))
            g
            full-nodes)))

(defn execution-result->dag [graph task result]
  (let [id      (:id task)
        updated (merge task result)]
    (cond->> updated
      :update-node-state (uber/set-attrs graph id)
      (failed? result)   (mark-downstream-failures id))))

(defn safe-run [f]
  (try
    {:state  :completed
     :result (f)}
    (catch Throwable t
      {:state :failed
       :error t})))

(defn execute [{:keys [db executions graph-id task]}]
  (let [{:keys [f id]} task
        result         (safe-run f)
        exec           (p/acquire-resource {:pool executions :id graph-id})
        graph          (get @db graph-id)
        new-graph      (execution-result->dag graph task result)]
    (swap! db assoc graph-id new-graph)
    (p/release-resource executions exec)))

(defn dag-finished? [g]
  (let [{:keys [pending running]} (nodes-by-state g)]
    (= 0
       (count pending)
       (count running))))

(defn work [{:keys [db executions]}]
  (when-let [{:keys [id] :as execution} (p/try-acquire {:pool executions})]
    (let [graph   (get @db id)
          task    (next-task graph)
          task-id (:id task)]
      (cond
        (some? task)
        (do
          (->> (assoc task :state :running)
               (uber/set-attrs graph task-id)
               (swap! db assoc id))
          (p/release-resource executions execution)
          (execute {:db         db
                    :executions executions
                    :graph-id   id
                    :task       task}))

        (dag-finished? graph)
        (swap! db assoc id graph)

        :else
        (p/release-resource executions execution)))))

(defn worker
  [{:keys [executions sleep db]
    :or   {sleep 1000}
    :as   params}]
  (loop []
    (try
      (work params)
      (Thread/sleep sleep)
      (catch Exception e
        (println e)))
    (recur)))

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

(def dag-1
  (->dag [[:a {:id    :a
               :f     (task-fn :a)
               :state :pending}]
          [:b {:id    :b
               :f     (task-fn :b)
               :state :pending}]
          [:c {:id    :c
               :f     (task-fn :c)
               :state :pending}]
          [:d {:id    :d
               :f     (task-fn :d)
               :state :pending}]]
         [[:a :b]
          [:a :c]
          [:c :d]
          [:b :d]]))

(def dag-2
  (->dag [[:e {:id    :e
               :f     (task-fn :e)
               :state :pending}]
          [:f {:id    :f
               :f     (task-fn :f)
               :state :pending}]
          [:g {:id    :g
               :f     (task-fn :g)
               :state :pending}]
          [:h {:id    :h
               :f     (task-fn :h)
               :state :pending}]]
         [[:e :f]
          [:e :g]
          [:g :h]
          [:f :h]]))

(def db
  (atom
   {1 dag-1
    2 dag-2}))

(def executions
  (atom [{:id 1} {:id 2}]))


(comment
  (dotimes [_ 2]
    (future (worker {:executions executions
                     :db         db}))))
