(ns bheap.queue-test
  (:require [clojure.test.check.generators :as gen]
            [stateful-check.core :as st]
            [stateful-check.symbolic-values :as symb]
            [clojure.test :refer [is]]))


(defn new-queue [] (atom clojure.lang.PersistentQueue/EMPTY))

(defn push-queue [queue val]
  (swap! queue conj val)
  nil)

(def new-queue-specification
  {:requires (fn [state] (nil? state))
   :command #'new-queue
   :next-state (fn [state _ result] {:queue result, :elements []})})

(def push-queue-specification
  {:requires (fn [state] state)
   :args (fn [state] [(:queue state) gen/nat])
   :command #'push-queue
   :next-state (fn [state [_ val] _] (update-in state [:elements] conj val))})

(defn pop-queue [queue]
  (swap! queue pop))

(def pop-queue-specification
  {:requires (fn [state] (seq (:elements state)))
   :args (fn [state] [(:queue state)])
   :command #'pop-queue
   :next-state (fn [state [queue] _] (update-in state [:elements] (comp vec next)))
   :postcondition (fn [state _ [_] val] (= (first (:elements state)) val))})

(def queue-spec
  {:commands {:new #'new-queue-specification
              #_#_#_#_:push #'push-queue-specification
              :pop #'pop-queue-specification}})
