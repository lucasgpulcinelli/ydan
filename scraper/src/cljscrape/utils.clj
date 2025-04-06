(ns cljscrape.utils
  (:import [java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers]
           [java.net URI]))

(def id2url
  {:video (partial format "https://www.youtube.com/watch?v=%s&gl=US&hl=en")
   :channel (partial format "https://www.youtube.com/channel/%s?gl=US&hl=en")})

(defn thumbnailurl [id] (format "https://i.ytimg.com/vi/%s/hqdefault.jpg" id))

(defn video? [id] (= 11 (count id)))

(defn channel? [id] (not (video? id)))

(defn kind [id] (if (video? id) :video :channel))

(defn request [url body-kind]
  (let [client (HttpClient/newHttpClient)
        bodyHandler (case body-kind
                      :string (HttpResponse$BodyHandlers/ofString)
                      :bytes (HttpResponse$BodyHandlers/ofByteArray))
        uri (URI/create url)
        req (-> (HttpRequest/newBuilder)
                (.uri uri)
                (.GET)
                (.build))
        resp (.send client req bodyHandler)]
    (if (== 200 (.statusCode resp))
      (.body resp)
      (throw (IllegalArgumentException.
              (format "response %d %s"
                      (.statusCode resp)
                      (.body resp)))))))

(defn get-match-from [pattern data]
  (let [matcher (.matcher pattern data)]
    (if (.find matcher)
      (.group matcher 1)
      (throw (IllegalArgumentException. "no match found")))))

(defn get-prop [_ _])

(defn get-element [data element]
  (if (vector? element) (map #(get-prop %1 element) data) (get data element)))

#_{:clj-kondo/ignore [:redefined-var]}
(defn get-prop [data path]
  (reduce get-element data path))

(defn rename-props [_ _])

(defn get-and-rename-prop [data path]
  (if (map? (last path))
    (map (partial rename-props (last path)) (get-prop data (drop-last path)))
    (get-prop data path)))

#_{:clj-kondo/ignore [:redefined-var]}
(defn rename-props [propmap data]
  (reduce-kv #(assoc %1 %2 (get-and-rename-prop data %3)) nil propmap))

(defn transform-props [transform-map data]
  (into
   {}
   (map
    #(vector
      (first %1)
      (if (second %1)
        ((get transform-map (first %1) identity) (second %1))
        (second %1)))
    data)))

