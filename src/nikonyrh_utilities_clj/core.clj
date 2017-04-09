(ns nikonyrh-utilities-clj.core
  (:require [java-time :as t]
            [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.string :as string]
            [clojure.java.io :as io])
  (:import java.util.zip.GZIPInputStream)
  (:gen-class))

(set! *warn-on-reflection* true)

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

(defn to-hex [data] (.substring (Integer/toString (+ (bit-and data 0xff) 0x100) 16) 1))
(defn sha1-hash [data] (->> (get-hash "sha1" data to-hex) (apply str)))
(defn sha1-hash-numeric [data] (get-hash "sha1" data int))

; http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
(defn my-println [& more]
  (do (.write *out* (str (t/local-time) " " (clojure.string/join "" more) "\n"))
      (flush)))

(defn getenv
  "Get environment variable's value, converts empty strings to nils"
  ([key]         (getenv key nil))
  ([key default] (let [value (System/getenv key)] (if (-> value count pos?) value default))))

(defn my-distinct
  "Returns distinct values from a seq, as defined by id-getter. Not thread safe!"
  [id-getter coll]
  (let [seen-ids (volatile! #{})
        seen?    (fn [id] (if-not (contains? @seen-ids id)
                            (vswap! seen-ids conj id)))]
    (filter (comp seen? id-getter) coll)))

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
      basic-parser     #(->> % (re-seq #"[0-9]+") (map int-parser) (apply t/local-date-time))]

  (def parsers
    {:int               int-parser
     :float             double-parser
     :latlon          #(if-not (= % "0") (double-parser %))
     :keyword         #(let [s (string/trim %)] (if-not (empty? s) s))
     :basic-datetime   (make-parser basic-parser)})

  (defn datetime-to-str [datetime]
    (let [d (->> datetime str (filter digits) (apply str))
          d (if (and (= (count d) 13) (= (nth d 8) \T))
              (str d "00")
              d)]
       (str (assert-len 15 d) "Z"))))

(defn day-of-week [^java.time.LocalDateTime datetime]
   (-> datetime .getDayOfWeek .getValue))

; This seemed like a good idea... note that .tar.gz file produces carbage to the beginning of the header row :(
(defmacro read-csv [type fname body]
  (let [decoder (case type
                  :gz   ['java.util.zip.GZIPInputStream.]
                  :zip  ['java.util.zip.ZipInputStream.]
                  :zlib ['java.util.zip.InflaterInputStream.]
                  [])]
    `(let [fname#  ~fname
           fname#  (if (= \/ (first fname#)) fname# (io/resource fname#))]
       (with-open [rdr# (-> fname# io/file io/input-stream ~@decoder)]
         (let [contents# (-> rdr# io/reader csv/read-csv)
               header#   (->> contents# first (map (comp keyword string/lower-case string/trim)))
               n-cols#   (count header#)
               ~'rows    (->> contents# rest (filter #(>= (count %) n-cols#)) (map #(zipmap header# %)))]
           ~body)))))
; (read-csv :csv "green_tripdata_2013-08.csv" (doall rows))

(comment
  (let [fname "green_tripdata_2013-08.csv"]
    (clojure.pprint/pprint
      [(read-csv :csv  fname               (-> rows first))
       (read-csv :gz   (str fname ".gz")   (-> rows first))
       (read-csv :zip  (str fname ".zip")  (-> rows first))
       (read-csv :zlib (str fname ".zlib") (-> rows first))])))

