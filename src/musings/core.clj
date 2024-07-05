(ns musings.core
  (:gen-class))

(defn my-fn [i]
  (if (zero? i)
    :end
    (recur (dec i))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
