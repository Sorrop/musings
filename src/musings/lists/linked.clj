(ns musings.lists.linked)


(defrecord LinkedListNode [id previous next data])

(defprotocol IDoubleLinkedList
  (head [this])
  (tail [this])
  (insert-after [this node data])
  (insert-before [this node data])
  (prepend [this data])
  (append [this data])
  (search [this target])
  (delete-node [this node])
  (get-backing-store [this])
  (to-vec [this]))

(deftype DoubleLinkedList [backing-store head-id tail-id]
  IDoubleLinkedList
  (head [_this] (get @backing-store @head-id))

  (tail [_this] (get @backing-store @tail-id))

  (insert-after [this node data]
    (let [new-node-id (random-uuid)
          old-node-id (:id node)
          new-node (->LinkedListNode new-node-id old-node-id nil data)]
      (if (= old-node-id @tail-id)
        (do (reset! tail-id new-node-id)
            (swap! backing-store
                   (fn [store]
                     (-> store
                         (assoc new-node-id new-node)
                         (assoc-in [old-node-id :next] new-node-id)))))
        (swap! backing-store
               (fn [store]
                 (let [old-node-next (get-in store [old-node-id :next])]
                   (-> store
                       (assoc-in [old-node-id :next] new-node-id)
                       (assoc new-node-id (assoc new-node :next old-node-next))
                       (assoc-in [old-node-next :previous] new-node-id))))))
      this))

  (insert-before [this node data]
    (let [new-node-id (random-uuid)
          old-node-id (:id node)
          new-node    (->LinkedListNode new-node-id nil old-node-id data)]
      (if (= old-node-id @head-id)
        (do (reset! head-id new-node-id)
            (swap! backing-store (fn [store]
                                   (-> store
                                       (assoc new-node-id new-node)
                                       (assoc-in [old-node-id :previous] new-node-id)))))
        (swap! backing-store
               (fn [store]
                 (let [old-node-prev (get-in store [old-node-id :previous])]
                   (-> store
                       (assoc-in [old-node-id :previous] new-node-id)
                       (assoc new-node-id (assoc new-node :previous old-node-prev))
                       (assoc-in [old-node-prev :next] new-node-id))))))
      this))

  (prepend [this data]
    (if (nil? (head this))
      (let [new-node-id (random-uuid)
            new-node (->LinkedListNode new-node-id nil nil data)]
        (reset! head-id new-node-id)
        (reset! tail-id new-node-id)
        (reset! backing-store (hash-map new-node-id new-node)))
      (insert-before this (head this) data))
    this)

  (append [this data]
    (if (nil? (tail this))
      (prepend this data)
      (insert-after this (tail this) data))
    this)

  (delete-node [this node]
    (let [node-id (:id node)]
      (if (nil? (:next node))
        (reset! tail-id (:previous node))
        (swap! backing-store (fn [store]
                               (let [prev (get node :previous)]
                                 (assoc-in store [prev :next] (:next node))))))
      (if (nil? (:previous node))
        (reset! head-id (:next node))
        (swap! backing-store (fn [store]
                               (let [next (get node :next)]
                                 (assoc-in store [next :previous] (:previous node))))))
      (swap! backing-store dissoc node-id))
    this)

  (get-backing-store [this] @backing-store)

  (search [this target]
    (let [store @backing-store]
      (loop [current (head this)]
        (let [{:keys [data next]} current]
          (when (some? data)
            (if (= data target)
              current
              (recur (get store next))))))))

  (to-vec [this]
    (let [store @backing-store]
      (loop [current (head this)
             out     []]
        (let [{:keys [data next]} current]
          (if (some? data)
            (recur (get store next)
                   (conj out data))
            out))))))

(defn create-linked-list []
  (->DoubleLinkedList (atom {}) (atom nil) (atom nil)))
