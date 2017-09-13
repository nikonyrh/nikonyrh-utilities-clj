(defproject org.clojars.nikonyrh.utilities-clj "1.0.0"
  :description "Clojure utilities"
  :url         "https://github.com/nikonyrh/nikonyrh-utilities-clj"
  :license {:name "Apache License, Version 2.0"
            :url  "http://www.apache.org/licenses/LICENSE-2.0"}
  :scm {:name "git"
        :url  "https://github.com/nikonyrh/nikonyrh-utilities-clj"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clojure.java-time "0.2.2"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/data.csv "0.1.3"]]
  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
  :aot [nikonyrh-utilities-clj.core]
  :main nikonyrh-utilities-clj.core)
