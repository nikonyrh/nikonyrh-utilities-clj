(ns nikonyrh-utilities-clj.core
  (:require [java-time :as t]
            [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.string :as string]
            [clojure.java.io :as io]
            
            [com.climate.claypoole :as cp])
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

(defmacro threadpool-defn [n-threads name & forms]
  `(let [~'threadpool (cp/threadpool ~(max 1 (eval n-threads)) :name ~(str "threadpool:" name))]
    (~'defn ~name ~@forms)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(let [regex #"\{([^:\{\}]+):([^\}]+)\}"]
  (defmacro my-format [fmt]
    (let [fmt (eval fmt)]
      `(format ~(clojure.string/replace fmt regex "$1")
               ~@(for [[_ type-str sym-str] (re-seq regex fmt)] (symbol sym-str))))))

(assert (= "test 123.46 case"         (let [a 123.456789] (my-format "test {%.2f:a} case"))))
(assert (= "a { test 123.46 case } b" (let [a 123.456789] (my-format "a { test {%.2f:a} case } b"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; To be used in the context of matching routes on Ring HTTP server.
(defmacro make-routes [request & args]
  `(condp #(some->> %2 :uri (re-find %) rest) ~request
     ~@(->> (for [[regex f-args f-body] (partition 3 args)] [regex :>> (list 'fn [f-args] f-body)])
            (apply concat))))
; (->> '(make-routes req #"a" [a] a #"b" [b] b) macroexpand-1 pprint)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Utilies for data processing

(defn my-distinct
  "Returns distinct values from a seq, as defined by id-getter."
  [id-getter coll]
  (let [seen-ids (atom #{})
        seen?    (fn [id] (when-not (contains? @seen-ids id)
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

; Ref. http://www.javacreed.com/how-to-generate-sha1-hash-value-of-file/
(defn sha1-file [fname]
  (with-open [in (clojure.java.io/input-stream fname)]
    (let [sha1   (java.security.MessageDigest/getInstance "sha1")
          buffer (byte-array (bit-shift-left 1 16))]
      (loop [n (.read in buffer)]
        (if (pos? n)
          (do (.update sha1 buffer 0 n)
              (recur (.read in buffer)))
          (->> (.digest sha1)
               (map to-hex)
               (apply str)))))))

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



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; A stand-alone implementation of SHA256, because why not? :)
; But dealing with signed integers wasn't fun.
(defn long->bytes [L]
  (let [L (long L)]
    (for [i [56 48 40 32 24 16 8 0]]
      (-> (Long/rotateRight L i)
          (bit-and 0xFF)))))

(let [min-integer (Integer/rotateLeft 1 31)]
  (defn long->int [^long l]
    (int (+ (if (>= l 2147483648) min-integer 0) (bit-and l 2147483647)))))

(defn bytes->int [[a b c d]]
  (long->int (+ (long d) (Long/rotateLeft c 8) (Long/rotateLeft b 16) (Long/rotateLeft a 24))))

(defn str->int32 [^String s]
  (let [L (-> s count (* 8))
        K (->> (mod (+ L 64) 512) (- 512 1 7))]
      (->> (concat s [(bit-shift-left 1 7)] (repeat (quot K 8) 0) (long->bytes L))
           (map int)
           (partition 4)
           (map bytes->int))))

(let [H-init (mapv long->int [0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19])
      K (->> (map  long->int [0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
                              0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
                              0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
                              0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
                              0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
                              0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
                              0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
                              0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2]) int-array)
      
      chunk-size      (quot 512 32)
      upper-bit       (bit-shift-left 1 31)
      lower-mask      (dec upper-bit)
      bit-not         (fn [i] (int (bit-xor i (int -1))))
      bit-xor         (comp int bit-xor)
      
      bit-shift-right (fn [a b] (bit-shift-right
                                  (+ (bit-and a lower-mask)
                                     (if (neg? a) upper-bit 0))
                                  b))
      
      W-extend-fn (fn W-extend-fn [i W delta rot0 rot1 shift0]
                    (let [w (W (+ i delta))]
                      (assert (<= -2147483648 w 2147483647))
                      (bit-xor (Integer/rotateRight w rot0)
                               (Integer/rotateRight w rot1)
                               (bit-shift-right     w shift0))))
      
      S-fn (fn S-fn [v i j k]
             (assert (<= -2147483648 v 2147483647))
             (bit-xor (Integer/rotateRight v i) (Integer/rotateRight v j) (Integer/rotateRight v k)))
      
      my-plus (fn [a b] (Integer/sum a (int b)))
      +       (fn [& coll] (reduce my-plus (int 0) coll))
      
      to-hex (fn [^Integer i] (format "%08x" i))]
  (defn sha256 [^String s]
    (let [input (->> s str->int32 (partition chunk-size))]
      (loop [[chunk & rest-input] input
             [h0 h1 h2 h3 h4 h5 h6 h7] H-init]
      ; (pprint [(map to-hex chunk) (map to-hex [h0 h1 h2 h3 h4 h5 h6 h7])])
        (if-not chunk
          (->> [h0 h1 h2 h3 h4 h5 h6 h7] (map to-hex) clojure.string/join)
          (let [W (loop [i chunk-size W (vec chunk)]
                    (if (= i 64) (int-array W)
                      (recur (inc i)
                             (conj W (+ (W (- i 16)) (W (- i 7))
                                        (W-extend-fn i W -15  7 18  3)
                                        (W-extend-fn i W  -2 17 19 10))))))]
          ; (prn (map to-hex W))
            (recur rest-input
              (map +
                [h0 h1 h2 h3 h4 h5 h6 h7]
                (loop [i 0 h h7 g h6 f h5 e h4 d h3 c h2 b h1 a h0]
                  (if (= i 64) [a b c d e f g h]
                    (let [S1 (S-fn e 6 11 25)
                          ch (bit-xor (bit-and e f) (bit-and (bit-not e) g))
                          t1 (+ h S1 ch (nth K i) (nth W i))
                          
                          S0 (S-fn a 2 13 22)
                          ma (bit-xor (bit-and a b) (bit-and a c) (bit-and b c))
                          t2 (+ S0 ma)]
                      (recur (inc i) g f e (+ d t1) c b a (+ t1 t2)))))))))))))


(comment
  (str->int32 "ABCD")
  (count (str->int32 "ABC"))
  
  (assert (= (sha256 "ABCDE") "f0393febe8baaa55e32f7be2a7cc180bf34e52137d99e056c817a9c07b8f239a")))
