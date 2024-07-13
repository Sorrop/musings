(ns musings.resource-pool)

(defn resource-pool [n create-fn]
  (atom (into [] (repeatedly n create-fn))))

(defn acquire-resource [pool]
  (loop [[r & rs :as all] @pool]
    (if (and (some? r)
             (compare-and-set! pool all rs))
      r
      (recur @pool))))

(defn try-acquire [pool]
  (let [[r & rs :as all] @pool]
    (when (and (some? r)
             (compare-and-set! pool all rs))
      r)))

(defn release-resource [pool r]
  (swap! pool conj r))

(defn test-it [n]
  (let [p       (resource-pool n 1)
        results (atom [])
        f       (fn [i]
                  (let [b (acquire-resource p)]
                    (aset b 0 (byte i))
                    (swap! results conj [i])
                    (release-resource p b)))]
    (doseq [i (range (inc n))]
      (future (f i)))
    (Thread/sleep 1000)
    {:pool    @p
     :results @results}))
