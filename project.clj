(defproject musings "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [ubergraph "0.9.0"]
                 [mvxcvi/clj-cbor "1.1.1"]]
  :main ^:skip-aot musings.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[org.clojars.czan/stateful-check "0.4.4"]
                                  [org.clojure/math.combinatorics "0.3.0"]
                                  [criterium "0.4.6"]]}})
