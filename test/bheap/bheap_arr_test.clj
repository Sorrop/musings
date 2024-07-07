(ns bheap.bheap-arr-test
  (:require [musings.bheap.protocol :as bheap]
            [musings.bheap.bheap-arr :as bheap-arr]
            [clojure.test.check.generators :as gen]
            [stateful-check.core :refer [specification-correct?]]
            [clojure.test :refer [is]]))

(def system-under-test
  (bheap-arr/create-bin-heap {:n 1024}))

(def insert-heap-spec
  {:args          (fn [_state] [gen/nat])
   :command       #(bheap/insert system-under-test %)
   :postcondition (fn [_prev _next-state [_val] _result]
                    (bheap-arr/is-heap? {:heap system-under-test}))})

(def extract-heap-spec
  {:command       #(bheap/extract system-under-test)
   :postcondition (fn [_prev _next _ _result]
                    (bheap-arr/is-heap? {:heap system-under-test}))})

(def delete-heap-spec
  {:args          (fn [_state] [gen/nat])
   :command       #(bheap/delete system-under-test %)
   :postcondition (fn [_ _ _ _] (bheap-arr/is-heap? {:heap system-under-test}))})

(def heap-spec
  {:commands {:insert  #'insert-heap-spec
              :extract #'extract-heap-spec
              :delete  #'delete-heap-spec}
   :setup    #(bheap/clear system-under-test)})


(defn test-bh []
  (is (specification-correct? heap-spec {:run    {:seed      12385347806
                                                  :max-tries 100}
                                         :gen    {:threads    2
                                                  :max-length 4}
                                         :report {:first-case? true}})))


(defn test-run []
  (let [heap (bheap-arr/create-bin-heap {:n 7})]
    (doseq [i [100 19 36 17 0]]
      (bheap/insert heap i))
    (println (bheap/to-vec heap))
    (bheap/extract heap)
    (println (bheap/to-vec heap))
    (bheap/extract heap)
    (println (bheap/to-vec heap))
    (bheap/extract heap)
    (println (bheap/to-vec heap))
    (bheap/extract heap)
    (println (bheap/to-vec heap))
    (bheap/extract heap)
    (println (bheap/to-vec heap))))

(defn test-random [n]
  (let [heap (bheap-arr/create-bin-heap {:n n})
        elems (shuffle (range n))]
    (doseq [i elems]
      (bheap/insert heap i))
    (doseq [i elems
            :let [heap? (bheap-arr/is-heap? {:heap heap})
                  v     (bheap/to-vec heap)]]
      (bheap/delete heap i)
      (println (format "is? %s i=%s\t v=%s" heap? i v))
      (when (not bheap-arr/is-heap?)
        (throw (RuntimeException. "not a heap: %s" v))))))

(defn multi-test []
  (let [heap (bheap-arr/create-bin-heap {:n 1024})]
    (doseq [t (shuffle (range 528))]
      (future (let [h-t (rand-nth [0 1])]
                (if (= 0 h-t)
                  (bheap/insert heap t)
                  (bheap/extract heap)))))

    (loop [i 0]
      (when (< i 50000)
        (let [v (bheap/to-vec heap)]
          (if (not (bheap-arr/v-is-heap? v compare))
            (throw (Exception. (format "not a heap: %s" v)))
            (recur (inc i))))))))
