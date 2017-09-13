(ns nikonyrh-utilities-clj.core
  (:require [java-time :as t]
            [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.string :as string]
            [clojure.java.io :as io])
  (:gen-class))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Wrappers for the magnificent "for" :D See macroexpands for details
(defmacro zipfor [i coll & forms]
  `(let [coll# ~coll]
     (let [c# coll#] (zipmap c# (for [~i c#] (do ~@forms))))))
; (->> '(zipfor i [1 2 3 4] (inc i)) macroexpand pprint)

(defmacro hashfor [& forms] `(->> (for ~@forms) (into {})))
; (macroexpand '(hashfor [i (range 10)] [(dec i) (inc i)]))

(defmacro catfor [seq-exprs body]
  (let [inner     (gensym)
        seq-exprs (conj seq-exprs inner body)]
    `(for ~seq-exprs ~inner)))
; (->> '(catfor [i (range 10)] (range i)) macroexpand-1 pprint)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Utility macros for parallelism, also have a look at https://github.com/TheClimateCorporation/claypoole

; Ref. https://stackoverflow.com/a/6697469/3731823
(defmacro with-timeout [millis & body]
  `(let [future# (future ~@body)]
    (try
      (.get future# ~millis java.util.concurrent.TimeUnit/MILLISECONDS)
      (catch java.util.concurrent.TimeoutException x# 
        (do
          (future-cancel future#)
          nil)))))
; (with-timeout 10 (println "start") (Thread/sleep 50) (println "end") true)

(defmacro dopar [& forms]
  (let [futures (->> (for [form forms] `(future ~form)) (into []))]
    `(->> ~futures (map deref) doall)))
; (->> '(dopar (do (Thread/sleep 200) (my-println "jee2")) (do (Thread/sleep 100) (my-println "jee1")))  macroexpand pprint)

(defmacro doparseq [seq-exprs & body]
  (let [futures `(for ~seq-exprs (future (do ~@body)))]
    `(doseq [f# ~futures] (deref f#))))
; (->> '(doparseq [i (range 5)] (Thread/sleep (->> i (* 100) (- 600))) (my-println i)) macroexpand-1 pprint)

(defmacro make-routes [request & args]
  `(condp #(some->> %2 :uri (re-find %) rest) ~request
     ~@(->> (for [[regex f-args f-body] (partition 3 args)] [regex :>> (list 'fn [f-args] f-body)])
            (apply concat))))
; To be used in the context of matching routes on Ring HTTP server.
; (->> '(make-routes req #"a" [a] a #"b" [b] b) macroexpand-1 pprint)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Utilies for data processing

(defn my-distinct
  "Returns distinct values from a seq, as defined by id-getter."
  [id-getter coll]
  (let [seen-ids (atom #{})
        seen?    (fn [id] (if-not (contains? @seen-ids id)
                            (swap! seen-ids conj id)))]
    (->> coll (filter (comp seen? id-getter)))))
; (my-distinct identity "abracadabra")
; (->> {:id (mod (* i i) 21) :value i} (for [i (range 50)]) (my-distinct :id) (map :value))

(defn make-sampler
  "Partitions input coll randomly into chunks of length n, returns them until exausted and then re-shuffles them."
  [n coll]
  (let [_       (assert (zero? (mod (count coll) n)) "Number of items must be divisible by the group size n!")
        reserve (atom [])]
    (fn [] (-> reserve
               (swap! (fn [reserve]
                         (if (<= (count reserve) 1)
                           (->> coll shuffle (partition n) (into []))
                           (subvec reserve 1))))
               first))))
; (repeatedly 10 (make-sampler 4 (range 12)))

; From https://gist.github.com/xpe/46128da5accb0acdf30d
(defn iterate-until-stable
  "Takes a function of one arg and calls (f x), (f (f x)), and so on
  until the value does not change."
  [x f] (loop [v x] (let [v' (f v)] (if (= v' v) v (recur v')))))

; https://gist.github.com/prasincs/827272
(defn get-hash [type data fun]
  (let [data (if (string? data) data (pr-str data))]
    (->>
      (.getBytes ^String data)
      (.digest (java.security.MessageDigest/getInstance type))
      (map fun))))

(defn to-hex            [data] (.substring (Integer/toString (+ (bit-and data 0xff) 0x100) 16) 1))
(defn sha1-hash         [data] (->> (get-hash "sha1" data to-hex) (apply str)))
(defn sha1-hash-numeric [data]      (get-hash "sha1" data int))

; Ref. https://stackoverflow.com/a/21404281/3731823
(defn periodically [f interval]
  (doto (Thread.
          #(try
             (while (not (.isInterrupted (Thread/currentThread)))
               (f)
               (Thread/sleep interval))
             (catch InterruptedException _)))
    (.start)))

(defn round
  ([^Double n] (round 3 n))
  ([^Integer p ^Double n]
   (let [p (Math/pow 10.0 p)]
     (when n (-> n (* p) Math/round (/ p))))))

; Ref. https://stackoverflow.com/a/6713290
(defn copy-to-clipboard [^String s]
  (-> (java.awt.Toolkit/getDefaultToolkit)
      .getSystemClipboard
      (.setContents (java.awt.datatransfer.StringSelection. s) nil)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Utilities for stuff like env variables, thread-safe timestamped printing and string parsing

; http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
(defn my-println
  "Print to *out* in a thread-safe manner"
  [& more]
  (do (.write *out* (str (clojure.string/join "" more) "\n"))
      (flush)))

(defn my-println-ts
  "Print to *out* in a thread-safe manner"
  [& more]
  (apply my-println (t/local-time) " " more))

(defn getenv
  "Get environment variable's value, treats empty strings as nils"
  ([key]         (getenv key nil))
  ([key default] (let [value (System/getenv key)] (if (empty? value) default value))))

; Ref. https://stackoverflow.com/a/35885
;      "Returns the name representing the running Java virtual machine. The returned name string can be any
;       arbitrary string and a Java virtual machine implementation can choose to embed platform-specific useful
;       information in the returned name string. Each running virtual machine could have a different name."
(defn get-pid []
  (-> (java.lang.management.ManagementFactory/getRuntimeMXBean) .getName (clojure.string/split #"@") first str (Integer.)))

(defmacro make-parser [f]
  `(fn [^String v#] (if-not (empty? v#)
                       (try (~f v#)
                            (catch Exception e#
                              (do (my-println "Invalid value '" v# "' for parser " (quote ~f))
                                  (my-println "Exception: " e#)))))))

(let [int-parser        (make-parser Integer.)
      double-parser     (make-parser Double.)
      digits            (into #{\T} (for [i (range 10)] (first (str i))))
      assert-len       #(if (= (count %2) %) %2 (throw (Exception. (str "Expected length " % ", got '" %2 "'!"))))
      datetime         #(->> % (re-seq #"[0-9]+") (map int-parser) (apply t/local-date-time))]

  (def parsers
    {:int               int-parser
     :float             double-parser
     :latlon          #(if-not (= % "0") (double-parser %))
     :keyword         #(let [s (string/trim %)] (if-not (empty? s) s))
     :datetime         (make-parser datetime)})

  (defn datetime-to-str [datetime]
    (let [d (->> datetime str (filter digits) (apply str))
          d (if (and (= (count d) 13) (= (nth d 8) \T))
              (str d "00")
              d)]
       (str (assert-len 15 d) "Z"))))

(defn day-of-week [^java.time.LocalDateTime datetime]
   (-> datetime .getDayOfWeek .getValue))

(defn datetime-to-epoch [^java.time.LocalDateTime dt]
  (-> dt (.atZone java.time.ZoneOffset/UTC) .toInstant .getEpochSecond))

(defn time-of-day
  "A float between 0.0 and 23.99972222222"
  [^java.time.LocalDateTime datetime]
  (-> datetime (t/as :second-of-day) (/ 3600.0)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; CSV parsing

(def write-csv csv/write-csv)

; At first this seemed like a good idea... This way you don't need to load
; the whole file into memory but you can instead process it lazily.
; Note that .tar.gz file produces carbage to the beginning of the header row :(
; fname can be either an absolute path, or a relative path within the resources folder.
(defmacro read-csv
  "Sets up a lazy sequence to rows symbol, which can be used in body as needed."
  [type fname body]
  (let [decoder (case type
                  :gz   ['java.util.zip.GZIPInputStream.]
                  :zip  ['java.util.zip.ZipInputStream.]
                  :zlib ['java.util.zip.InflaterInputStream.]
                  [])]
    `(let [fname-orig#   ~fname
         ; TODO: Windows vs. Linux/Unix check. Basically this tries to identify absolute paths
           fname#   (-> (if (or (= \/ (nth fname-orig# 0))
                                (= \: (nth fname-orig# 1)))
                          fname-orig#
                          (io/resource fname-orig#))
                        io/file)]
       (assert (some?   fname#) (str "File '" fname-orig# "' not found from resources!"))
       (assert (.exists fname#) (str "File '" fname#      "' not found!"))
       (with-open [rdr# (-> fname# io/input-stream ~@decoder)]
         (let [_#        (if (= :zip ~type) (.getNextEntry ^java.util.zip.ZipInputStream rdr#))
               contents# (-> rdr# io/reader csv/read-csv)
               header#   (->> contents# first (map (comp keyword string/lower-case string/trim)))
               n-cols#   (count header#)
               ~'rows    (->> contents# rest (filter #(>= (count %) n-cols#))
                                             (map    #(zipmap header# %)))]
           ~body)))))

(comment
  "The body can be arbitrary code which uses the 'rows' symbol"
  (let [fname "green_tripdata_2013-08.csv"]
    (clojure.pprint/pprint
      [(read-csv :csv  fname               (->  rows first))
       (read-csv :gz   (str fname ".gz")   (->  rows count))
       (read-csv :zip  (str fname ".zip")  (->  rows first keys sort))
       (read-csv :zlib (str fname ".zlib") (->> rows first (into (sorted-map))))])))


(defmacro read-csv-with-mapping
  "Provide a mapping from column keywords to pre-defined functions at 'parsers', or bring your own."
  [type fname mapping body]
  `(let [mapping# (into [] (for [[k# v#] ~mapping]
                             (if-not (sequential? v#)
                               [k# k# (if (keyword? v#) (~'parsers v#) v#)]
                               (let [[fv# sv#] v#]
                                 [k# sv# (if (keyword? fv#) (~'parsers fv#) fv#)]))))
         missing-parsers#   (->> (for [[k-in# k-out# parser#] mapping# :when (nil? parser#)] {:from k-in# :to k-out#}) (into []))
         _#                 (if-not (empty? missing-parsers#)
                              (throw (Exception. (str "Some columns have unidentified parsers: " missing-parsers#))))]
    (read-csv ~type ~fname
      (let [orig-cols# (-> ~'rows first keys set)
            ~'rows (for [row# ~'rows]
                     (hashfor [[k-in# k-out# parser#] mapping#]
                       (if (contains? row# k-in#)
                         [k-out# (-> k-in# row# parser#)])))
            mapping-source-cols# (->> mapping# (map first) set)
            mapping-target-cols# (->> mapping# (map second) set)
            mapped-cols#         (-> ~'rows first keys set)
            missing#             (clojure.set/difference mapping-target-cols# mapped-cols#)
            _#        (if-not (empty? missing#)
                        (do
                         (my-println "")
                         (my-println "Existing columns in CSV: "        (sort orig-cols#))
                         (my-println "Source columns in mapping: "      (sort mapping-source-cols#))
                         (my-println "Target columns in mapping: "      (sort mapping-target-cols#))
                         (my-println "Existing columns after mapping: " (sort mapped-cols#))
                         (throw (Exception. (str "Columns " missing# " not found from " ~fname)))))]
        ~body))))

(comment
  (read-csv-with-mapping :csv "green_tripdata_2013-08.csv" {:not-found :float} (first rows))
  (read-csv-with-mapping :csv "green_tripdata_2013-08.csv" {:tip_amount :not-found} (first rows))
  
  (let [to-float (fn [^String s] (if-not (empty? s) (Float. s)))
        fname    "green_tripdata_2013-08.csv"
        mapping {:pickup_latitude       [:latlon   :lat]
                 :pickup_longitude      [to-float  :lon]
                 :lpep_pickup_datetime  [:datetime :pickup_dt]
                 :passenger_count        :int
                 :fare_amount            :float
                 :total_amount            to-float
                 :store_and_fwd_flag     :keyword
                 :trip_type              :keyword}]
    (clojure.pprint/pprint
      [(read-csv              :csv fname         (nth rows 8))
       (read-csv-with-mapping :csv fname mapping (nth rows 8))])))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; This masterpiece is a helpful wrapper when you've got an exception but you
; lost track on what is actually going on. If the "form" causes an exception
; then this annotates it with specific values of all symbols in the form.
; Note that it cannot wrap let or for clauses, as not all used symbols can
; be resolved at the "root" context.
(defmacro with-catch [form]
  (let [symbols     (->> form flatten (filter symbol?) (remove resolve) set (into []))
        symbol-strs (->> symbols (map str) (into []))]
    `(try ~form
       (catch Exception e#
         (let [info# {:form    (read-string ~(str form))
                      :symbols (into (sorted-map) (zipmap (map symbol ~symbol-strs) ~symbols))}]
           (clojure.pprint/pprint info#)
           (throw (ex-info (str e#) (assoc info# :exception e#))))))))

; (->> '(with-catch (+ a b c)) macroexpand pprint))

; (for [[i j] (partition 2 1 [1 2 3 4 "5" 6 7])] (with-catch (+ i j)))
; ExceptionInfo java.lang.ClassCastException: java.lang.String cannot be cast to java.lang.Number

; (-> *e ex-data (dissoc :exception) pprint)
; {:form (+ i j), :symbols {i 4, j "5"}}
