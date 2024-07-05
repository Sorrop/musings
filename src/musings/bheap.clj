(ns musings.bheap
  )

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

  (to-vec [heap]
    "Returns the backing array as a vector"))

(defn place-leftmost! [arr elem]
  (let [n (alength arr)]
    (loop [i 0]
      #_(cond
        (>= i n) :full
        (nil? (aget arr i)) (aset arr i elem)

        (and (< (* i 2) n)
             (nil? (aget arr (* i 2))))
        (aset arr (* i 2) elem)

        (and (< (inc (* i 2)) n)
             (nil? (aget arr (inc (* i 2)))))
        (aset arr (inc (* i 2)) elem)

        :else (recur ))
      (cond
          (> i n)             :full
          (nil? (aget arr i)) (do (aset arr i elem)
                                  i)
          :else               (recur (inc i))))))

(defn get-last [arr]
  (let [n (alength arr)]
    (loop [i (dec n)]
      (if (nil? (aget arr i))
        (recur (dec i))
        i))))

(defn aswap! [arr i j]
  (let [a-i (aget arr i)]
    (aset arr i (aget arr j))
    (aset arr j a-i)))

(defn max-comp [a b]
  (cond
    (= a b) 0
    (< a b) (- 1)
    :else   1))

(defn find-elem [arr elem]
  (let [n (alength arr)]
    (loop [i 0]
      (cond
        (= i n)               nil
        (= (aget arr i) elem) i
        :else                 (recur (inc i))))))

(deftype BinHeap [compare-fn arr]
  BHeapProtocol

  (insert [this element]
    (let [ind (place-leftmost! arr element)]
      (if (= ind :full)
        (throw (RuntimeException. "Heap full"))
        (loop [i ind]
          (let [parent-i (quot i 2)
                parent   (aget arr parent-i)]
            (when (< (compare-fn element parent) 0)
              (aswap! arr i parent-i)
              (recur parent-i)))))))

  (extract [this]
    (let [element (aget arr 0)]
      (if (nil? element)
        (throw (RuntimeException. "Heap empty"))
        ;; swap root with last element
        (let [last-i (get-last arr)]
          (aset arr 0 nil)
          (aswap! arr 0 last-i)
          ;; preserve heap property for root recursively
          (loop [root (aget arr 0)]
            (let [left-result (compare-fn root (aget arr 1))]
              (cond
                (>= left-result 0) (do (aswap! arr 0 1) (recur (aget arr 0)))
                :else
                (let [right-result (compare-fn root (aget arr 2))]
                  (when (>= right-result 0)
                    (aswap! arr 0 2)
                    (recur (aget arr 0)))))))))
      element))

  (delete [this element]
    (let [elem-i (find-elem arr element)
          last-i (get-last arr)]
      ;; swap found element with the last
      (aswap! arr elem-i last-i)
      (aset arr last-i nil)
      (when (not= elem-i last-i)
        ;; check if swapped element violates heap property with parent
        (loop [pos      elem-i
               parent-i (quot (dec elem-i) 2)]
          (let [size   (alength arr)
                result (compare-fn (aget arr pos) (aget arr parent-i))]
            (if (< result 0)
              ;; if yes swap and recur from there
              (do
                (aswap! arr pos parent-i)
                (recur parent-i (quot (dec parent-i) 2)))

              ;; else check if swapped element violates heap property
              ;; with children and recur
              (let [l-child (inc (* pos 2))
                    r-child (inc l-child)]
                (when (< l-child size)
                  (if (and (some? (aget arr l-child))
                           (pos? (compare-fn (aget arr elem-i)
                                             (aget arr l-child))))
                    (do (aswap! arr elem-i l-child)
                        (recur l-child pos))
                    (when (and (< r-child size)
                               (some? (aget arr r-child))
                               (pos? (compare-fn (aget arr elem-i)
                                                 (aget arr r-child))))
                      (recur r-child pos)))))))))))

  (update-elem [this element updater] this)

  (to-vec [this] (vec arr)))


(defn create-bin-heap [comparator n]
  (BinHeap. comparator (make-array Object n)))


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
