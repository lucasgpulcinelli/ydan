(ns cljscrape.utils
  (:import [java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers]
           [java.net URI]))

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

