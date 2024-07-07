(ns musings.bheap.bheap-arr
  (:require [musings.bheap.protocol :as prot])
  (:import (java.util ArrayList)
           (java.util Collections)))

(defn place-leftmost! [^ArrayList arr elem]
  (.add arr elem)
  (dec (.size arr)))

(defn aswap! [^ArrayList arr ^Integer i ^Integer j]
  (let [a-i (.get arr i)]
    (.set arr i (.get arr j))
    (.set arr j a-i)))

(defn find-elem [^ArrayList arr elem compare-fn]
  (let [n (.size arr)]
    (loop [i 0]
      (cond
        (>= i n)                                nil
        (zero? (compare-fn (.get arr i) elem)) i
        :else                                  (recur (inc i))))))

(defn smaller-child [^ArrayList arr compare-fn left right]
  (if (neg? (compare-fn (.get arr left) (.get arr right)))
    left
    right))

(defn down-heap! [^ArrayList arr compare-fn elem-i]
  (let [total (.size arr)]
    (loop [pos elem-i]
      (let [l-child     (inc (* pos 2))
            r-child     (inc l-child)
            has-l-child (< l-child total)
            has-r-child (< r-child total)]
        ;; swap with smaller child
        (cond
          (and has-l-child
               has-r-child)
          (let [smaller (smaller-child arr compare-fn l-child r-child)]
            (when (pos? (compare-fn (.get arr pos) (.get arr smaller)))
              (aswap! arr pos smaller)
              (recur smaller)))

          has-l-child
          (when (pos? (compare-fn (.get arr pos)
                                  (.get arr l-child)))
            (aswap! arr pos l-child)
            (recur l-child))

          :else
          (when (and has-r-child
                     (pos? (compare-fn (.get arr pos)
                                       (.get arr r-child))))
            (aswap! arr pos r-child)
            (recur r-child)))))))

(defn up-heap! [^ArrayList arr compare-fn i]
  (loop [pos i]
    (let [parent (quot (dec pos) 2)
          result (compare-fn (.get arr pos) (.get arr parent))]
      (when (neg? result)
        (aswap! arr pos parent)
        (recur parent)))))

(defn preserve-heap! [^ArrayList arr compare-fn elem-i]
  (let [parent-i (quot (dec elem-i) 2)
        result   (compare-fn (.get arr elem-i) (.get arr parent-i))]
    (if (< result 0)
      ;; if yes swap and recur from there
      (do
        (aswap! arr elem-i parent-i)
        (up-heap! arr compare-fn parent-i))

      ;; else check if swapped element violates heap property
      ;; with children and recur
      (down-heap! arr compare-fn elem-i))))

(deftype BinHeap [compare-fn ^ArrayList arr]
  prot/BHeapProtocol

  (insert [_this element]
    (locking arr
      (let [ind (place-leftmost! arr element)]
        (loop [i ind]
          (let [parent-i (quot (dec i) 2)
                parent   (.get arr parent-i)]
            (when (< (compare-fn element parent) 0)
              (aswap! arr i parent-i)
              (recur parent-i)))))))

  (extract [_this]
    (locking arr
      (when-not (.isEmpty arr)
        (let [element (.get arr 0)
              last-i  (dec (.size arr))]
          ;; swap root with last element
          (aswap! arr 0 last-i)
          (.remove arr last-i)
          (case last-i
            ;; nothing to be done if extracting from 1 or 2 element heap
            (0 1)  nil
            ;; might need swapping if extracting from 3 element heap
            2      (let [res (compare-fn (.get arr 0) (.get arr 1))]
                     (when (pos? res)
                       (aswap! arr 0 1)))

            ;; otherwise preserve heap property recursively
            (down-heap! arr compare-fn 0))
          element))))

  (delete [_this element]
    (locking arr
      (when-not (.isEmpty arr)
        (let [last-i (dec (.size arr))
              elem-i (find-elem arr element compare-fn)]
          (when (some? elem-i)
          ;; swap found element with the last
            (aswap! arr elem-i last-i)
            (.remove arr last-i)
            (when-not (= elem-i last-i)
              (preserve-heap! arr compare-fn elem-i)))))))

  (update-elem [this element updater]
    (locking arr
      (when-not (.isEmpty arr)
        (let [elem-i (find-elem arr element compare-fn)]
          (when (some? elem-i)
            (let [old-v (.get arr elem-i)
                  new-v (updater (.get arr elem-i))]
              (.set arr elem-i new-v)
              (if (pos? (compare-fn new-v old-v))
                (down-heap! arr compare-fn elem-i)
                (up-heap! arr compare-fn elem-i))))))))

  (to-vec [_this] (locking arr (vec arr)))

  (size [_this] (.size arr))

  (has? [_this elem] (find-elem arr elem compare-fn))

  (clear [_this] (locking arr (.clear arr))))


(defn create-bin-heap [{:keys [comparator n]
                        :or   {comparator compare}}]
   (BinHeap. comparator (ArrayList. n)))


(defn is-leftmost-filled? [v]
  (= (into [] (concat (filter some? v)
                      (filter nil? v)))
     v))

(defn v-is-heap? [v compare-fn]
  (or (zero? (count v))
      (and (is-leftmost-filled? v)
           (let [fv (filterv some? v)]
             (loop [i (dec (count fv))]
               (let [parent (quot (dec i) 2)]
                 (cond
                   (pos? (compare-fn (get fv parent)
                                     (get fv i)))
                   false

                   (zero? i) true
                   :else     (recur (dec i)))))))))

(defn is-heap? [{:keys [heap compare-fn]
                 :or {compare-fn compare}}]
  (v-is-heap? (prot/to-vec heap) compare-fn))

