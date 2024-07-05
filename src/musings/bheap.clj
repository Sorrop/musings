(ns musings.bheap
  (:import (java.util ArrayList)))

(defprotocol BHeapProtocol
  (insert [heap element]
    "Inserts element to heap")

  (extract [heap]
    "Extracts the root of the heap")

  (delete [heap element]
    "Delete arbitrary element from heap")

  (update-elem [heap element updater]
    "Update the specified element in the heap based on the updater fn
     and maintain heap property")

  (size [heap]
    "Returns the size of the underlying ArrayList")

  (to-vec [heap]
    "Returns the backing array as a vector"))

(defn place-leftmost! [^ArrayList arr elem]
  (.add arr elem)
  (dec (.size arr)))

(defn aswap! [^ArrayList arr i j]
  (let [a-i (.get arr i)]
    (.set arr i (.get arr j))
    (.set arr j a-i)))

(defn max-comp [a b]
  (cond
    (= a b) 0
    (< a b) (- 1)
    :else   1))

(defn find-elem [^ArrayList arr elem]
  (let [n (.size arr)]
    (loop [i 0]
      (cond
        (= i n)               nil
        (= (.get arr i) elem) i
        :else                 (recur (inc i))))))

(deftype BinHeap [compare-fn ^ArrayList arr]
  BHeapProtocol

  (insert [_this element]
    (let [ind (place-leftmost! arr element)]
      (loop [i ind]
        (let [parent-i (quot (dec i) 2)
              parent   (.get arr parent-i)]
          (when (< (compare-fn element parent) 0)
            (aswap! arr i parent-i)
            (recur parent-i))))))

  (extract [_this]
    (when-not (.isEmpty arr)
      (let [element (.get arr 0)
            last-i  (dec (.size arr))]
        ;; swap root with last element
        (aswap! arr 0 last-i)
        (.remove arr last-i)
        ;; preserve heap property for root recursively
        (loop [root (.get arr 0)]
          (let [left-result (compare-fn root (.get arr 1))]
            (cond
              (>= left-result 0) (do (aswap! arr 0 1) (recur (.get arr 0)))
              :else
              (let [right-result (compare-fn root (.get arr 2))]
                (when (>= right-result 0)
                  (aswap! arr 0 2)
                  (recur (.get arr 0)))))))
        element)))

  (delete [_this element]
    (when-not (.isEmpty arr)
      (let [elem-i (find-elem arr element)
            last-i (dec (.size arr))]
      ;; swap found element with the last
        (aswap! arr elem-i last-i)
        (.remove arr last-i)
        (let [size (.size arr)]
          (when (not= elem-i last-i)
          ;; check if swapped element violates heap property with parent
            (loop [pos      elem-i
                   parent-i (quot (dec elem-i) 2)]
              (let [result (compare-fn (.get arr pos) (.get arr parent-i))]
                (if (< result 0)
                ;; if yes swap and recur from there
                  (do
                    (aswap! arr pos parent-i)
                    (recur parent-i (quot (dec parent-i) 2)))

                ;; else check if swapped element violates heap property
                ;; with children and recur
                  (let [l-child (inc (* pos 2))
                        r-child (inc l-child)]
                    (if (and (< l-child size)
                             (pos? (compare-fn (.get arr elem-i)
                                               (.get arr l-child))))
                      (do (aswap! arr elem-i l-child)
                          (recur l-child pos))
                      (when (and (< r-child size)
                                 (pos? (compare-fn (.get arr elem-i)
                                                   (.get arr r-child))))
                        (recur r-child pos))))))))))))

  (update-elem [this _element _updater] this)

  (to-vec [_this] (vec arr))

  (size [_this] (.size arr)))


(defn create-bin-heap [comparator n]
  (BinHeap. comparator (ArrayList. n)))


(defn is-leftmost-filled? [v]
  (= (into [] (concat (filter some? v)
                      (filter nil? v)))
     v))

(defn is-heap? [heap compare-fn]
  (let [v (to-vec heap)]
    (and (is-leftmost-filled? v)
         (let [fv (filter some? v)]
           (loop [i (dec (count fv))]
             (let [parent (quot (dec i) 2)]
               (cond
                 (pos? (compare-fn (get fv parent)
                                   (get fv i)))
                 false

                 (zero? i) true
                 :else     (recur (dec i)))))))))


(comment
  (let [heap (create-bin-heap max-comp 7)]
    (doseq [i [100 19 36 17 0]]
      (insert heap i))
    (println (to-vec heap))
    (extract heap)
    (println (to-vec heap))
    (delete heap 17)
    (println (to-vec heap))
    (delete heap 19)
    (to-vec heap))

  (let [heap (create-bin-heap max-comp 100)]
    (doseq [i (shuffle (range 100))]
      (insert heap i))
    (is-heap? heap max-comp)))
