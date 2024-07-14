(ns musings.resource-pool)

(defn resource-pool [n create-fn]
  (atom (into [] (repeatedly n create-fn))))

(defn select-id [pool id]
  (if (some? id)
    (loop [[r & rs] pool
           result {:selected nil
                   :rest     []
                   :all      pool}]
      (if (some? r)
        (if (= (:id r) id)
          (-> (assoc result :selected r)
              (update :rest (comp vec concat) rs))
          (recur rs (update result :rest conj r)))
        (update result :rest (comp vec concat) rs)))
    {:selected (first pool)
     :rest     (vec (rest pool))
     :all      pool}))

(defn try-acquire [{:keys [pool id]}]
  (let [{:keys [selected rest all]} (select-id @pool id)]
    (when (and (some? selected)
               (compare-and-set! pool all rest))
      selected)))

(defn acquire-resource [params]
  (loop []
    (if-let [r (try-acquire params)]
      r
      (recur))))



(defn release-resource [pool r]
  (swap! pool conj r))

(defn test-it [n]
  (let [p       (resource-pool n 1)
        results (atom [])
        f       (fn [i]
                  (let [b (acquire-resource {:pool p})]
                    (aset b 0 (byte i))
                    (swap! results conj [i])
                    (release-resource p b)))]
    (doseq [i (range (inc n))]
      (future (f i)))
    (Thread/sleep 1000)
    {:pool    @p
     :results @results}))
