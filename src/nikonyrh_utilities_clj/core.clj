(ns nikonyrh-utilities-clj.core
  (:require [java-time :as t]
            [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.string :as string]
            [clojure.java.io :as io])
  (:gen-class))

(set! *warn-on-reflection* true)


(defmacro zipfor [i coll & forms]
  `(let [c# ~coll] (zipmap c# (for [~i c#] (do ~@forms)))))
; (macroexpand '(zipfor i [1 2 3 4] (inc i)))


(defmacro hashfor [& forms] `(->> (for ~@forms) (into {})))
; (macroexpand '(hashfor [i (range 10)] [(dec i) (inc i)]))


(def write-csv csv/write-csv)

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

; http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
(defn my-println
  "Print to *out* in a thread-safe manner"
  [& more]
  (do (.write *out* (str (t/local-time) " " (clojure.string/join "" more) "\n"))
      (flush)))

(defn getenv
  "Get environment variable's value, treats empty strings as nils"
  ([key]         (getenv key nil))
  ([key default] (let [value (System/getenv key)] (if (empty? value) default value))))

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


; Ref. https://stackoverflow.com/a/9200642/3731823
(defn glob
  "Find files and folders within a path, filter by re-seq, returns absolute path strings"
  [folder re]
  (->> folder io/file file-seq
       (map (fn [^java.io.File f] (.getAbsolutePath f)))
       (filter #(re-find re %))))

; Ref. https://stackoverflow.com/a/21404281/3731823
(defn periodically [f interval]
  (doto (Thread.
          #(try
             (while (not (.isInterrupted (Thread/currentThread)))
               (f)
               (Thread/sleep interval))
             (catch InterruptedException _)))
    (.start)))

; Ref. https://stackoverflow.com/a/35885
;      "Returns the name representing the running Java virtual machine. The returned name string can be any
;       arbitrary string and a Java virtual machine implementation can choose to embed platform-specific useful
;       information in the returned name string. Each running virtual machine could have a different name."
(defn get-pid []
  (-> (java.lang.management.ManagementFactory/getRuntimeMXBean) .getName (clojure.string/split #"@") first str (Integer.)))


(defn round
  ([^Double n] (round 3 n))
  ([^Integer p ^Double n]
   (let [p (Math/pow 10.0 p)]
     (when n (-> n (* p) Math/round (/ p))))))


(defmacro make-routes [request & args]
  `(condp #(some->> %2 :uri (re-find %) rest) ~request
     ~@(->> (for [[regex f-args f-body] (partition 3 args)] [regex :>> (list 'fn [f-args] f-body)])
            (apply concat))))
; To be used in the context of matching routes on Ring HTTP server.
; (->> '(make-routes req #"a" [a] a #"b" [b] b) macroexpand-1 pprint)
