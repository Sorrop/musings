(ns musings.lists.linked-test
  (:require [clojure.test :as t]
            [musings.lists.linked :as sut]
            [clojure.test.check.generators :as gen]))

(def set-and-elem
  (gen/bind (gen/not-empty (gen/set gen/small-integer {:num-elements 10}))
            #(gen/tuple (gen/return %)
                        (gen/elements %))))

(defn gen-vector-elem-idx [num-samples]
  (mapv (fn [[s elem]]
          (let [v (vec s)]
            [v elem (.indexOf v elem)]))
        (gen/sample set-and-elem num-samples)))

(t/deftest list-insertion
  (t/testing "list prepend"
    (let [l (sut/create-linked-list)]
      (doseq [x (range 100)]
        (sut/prepend l x)
        (t/is (= x (-> l sut/head :data))))))
  (t/testing "list append"
    (let [l (sut/create-linked-list)
          input (vec  (range 100))]
      (doseq [x (range 100)]
        (sut/append l x)
        (t/is (= x (-> l sut/tail :data))))
      (t/is (= input (sut/to-vec l)))))
  (t/testing "random insertion before"
    (doseq [input (gen-vector-elem-idx 100)
            :let [[v elem idx] input
                  my-l (reduce sut/append (sut/create-linked-list) v)
                  correct (vec (concat (subvec v 0 idx) [:flag] (subvec v idx)))
                  node (sut/search my-l elem)]]
      (sut/insert-before my-l node :flag)
      (t/is (= correct (sut/to-vec my-l)))))
  (t/testing "random insertion after"
    (doseq [input (gen-vector-elem-idx 100)
            :let [[v elem idx] input
                  my-l (reduce sut/append (sut/create-linked-list) v)
                  cut (inc idx)
                  correct (vec (concat (subvec v 0 cut) [:flag] (subvec v cut)))
                  node (sut/search my-l elem)]]
      (sut/insert-after my-l node :flag)
      (t/is (= correct (sut/to-vec my-l))))))
