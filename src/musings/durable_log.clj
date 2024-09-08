(ns musings.durable-log
  (:require [clojure.java.io :as io]
            [clj-cbor.core :as cbor])
  (:import (java.io FileOutputStream FileInputStream)
           (java.nio ByteBuffer)
           (java.nio.file Files Paths)
           (java.nio.channels FileChannel)))

(defn flush-log-locking
  "
  - Cooperative locking on the file backed by OS advisory locks.
  - OpenJDK in linux uses fcntl(2) for advisory locks on files.
    They are bound to processes, not file descriptors.
  - This lock will be visible to other processes in linux that use the
    same underlying system call, not flock().
  "
  [log-file byte-buffer]
  (with-open [^FileOutputStream fous (FileOutputStream. (io/file log-file) true)
              ^FileChannel channel   (.getChannel fous)]
    (let [lock (.lock channel)]
      (try
        (.write channel byte-buffer)
        (finally (.release lock))))))

(defn buffered-content [{:keys [buffered-bytes content]}]
  (reduce (fn [byte-buffer barr]
            (.put byte-buffer barr))
          (ByteBuffer/allocate buffered-bytes)
          content))

(defn flush-log
  [log-file content]
  (with-open [os (FileOutputStream. (io/file log-file) true)]
    (doseq [barr content]
      (.write os barr))))

(defn append-log
  [buffer lock event]
  (let [encoded (cbor/encode event)
        length  (alength encoded)]
    (locking lock
      (swap! buffer (fn [{:keys [buffered-bytes content]}]
                      {:buffered-bytes (+ buffered-bytes length)
                       :content (conj content encoded)})))))

(defn read-log-file [log-file]
  (with-open [is (io/input-stream log-file)]
    (into [] (cbor/decode-seq is))))

(defprotocol DurableAppendLog
  (append [this event])

  (read-log [this])

  (get-size [this])

  (flush-it [this]))

(defrecord DLog  [log-file buffer lock]
  DurableAppendLog
  (append [_this event]
    (append-log buffer lock event))

  (read-log [_this]
    (read-log-file log-file))

  (get-size [_this]
    (-> log-file
        (Paths/get (into-array String []))
        Files/size))

  (flush-it [_this]
    (when (> (count (:content @buffer)) 0)
      (let [content (locking lock
                      (let [{:keys [content]}  @buffer]
                        (swap! buffer assoc
                               :buffered-bytes 0
                               :content        [])
                        content))]
        (flush-log log-file content)))))

(defn init-log
  [{:keys [log-file flush-period]
    :or {flush-period 1000}}]
  (let [bf  (atom {:buffered-bytes 0
                   :content        []})
        log (->DLog log-file bf (Object.))]
    {:log log
     :flush-thread (future
                     (loop []
                       (try (.flush-it log)
                            (catch Throwable t
                              (println t)))
                       (Thread/sleep flush-period)
                       (recur)))}))

(comment
  (def my-log (init-log {:log-file "mylog"}))

  (def my-log
    (let [buffer (atom {:buffered-bytes 0
                        :content        []})]
      (->DLog "mylog" buffer (Object.))))

  (def flushing-thread
    (future (loop []
              (try (.flush-it my-log)
                   (catch Throwable t (println t)))
              (Thread/sleep 1000)
              (recur)))))


;; ============= SCRATCHPAD STUFF =================


(defn write-n [log process threads wait]
  (doseq [t (range threads)]
    (future
      (Thread/sleep (rand-int wait))
      (try
        (.append log {:a t :process process})
        (catch Exception e
          (println e))))))

