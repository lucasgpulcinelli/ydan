(ns cljscrape.store
  (:require [cheshire.core :as json]
            [cljscrape.utils :as utils]
            [cljscrape.constants :as consts])
  (:import [java.util Base64]
           [java.net URI]
           [software.amazon.awssdk.services.s3 S3Configuration]
           [java.util.function Consumer]
           [software.amazon.awssdk.regions Region]
           [software.amazon.awssdk.services.s3 S3Client]
           [software.amazon.awssdk.core.sync RequestBody]
           [software.amazon.awssdk.auth.credentials AwsBasicCredentials StaticCredentialsProvider]
           [software.amazon.awssdk.services.s3.model PutObjectRequest]))

(def s3-client
  (->
   (S3Client/builder)
   (.endpointOverride (URI. consts/minio-url))
   (.credentialsProvider
    (StaticCredentialsProvider/create
     (AwsBasicCredentials/create consts/minio-user consts/minio-pass)))
   (.region (Region/of "us-east-1"))
   (.serviceConfiguration
    (reify Consumer
      (accept [_ builder]
        (S3Configuration/builder)
        (.pathStyleAccessEnabled builder true))))
   (.build)))

(defn save-to-s3 [bucket key content]
  (let [request (->
                 (PutObjectRequest/builder)
                 (.bucket bucket)
                 (.key key)
                 (.build))
        body (RequestBody/fromBytes content)]
    (.putObject s3-client request body)))

(defn save-entry [entry]
  (let [id (:id entry)
        data (:data entry)
        caps (get data "captions")
        img (get data "thumbnail")
        encoded-id (->> id (.getBytes) (.encode (Base64/getEncoder)) (String.))
        bucket-partition (mod (.hashCode id) consts/partition-amount)]

    (and
     (utils/video? id)
     (do
       (and (not (nil? img))
            (save-to-s3
             consts/minio-bucket
             (format "thumbnails/%d/%s.jpeg" bucket-partition encoded-id)
             img))

       (and (not (nil? caps))
            (save-to-s3
             consts/minio-bucket
             (format "captions/%d/%s.xml" bucket-partition encoded-id)
             (.getBytes (second caps) "UTF-8")))))

    (as-> data v
      (dissoc v "thumbnail")
      (if (not (nil? caps)) (assoc (dissoc v "captions") "caption_language" (first caps)) v)
      (json/encode v)
      (.getBytes v "UTF-8")
      (save-to-s3
       consts/minio-bucket
       (format "%s_data/%d/%s.json"
               (-> id (utils/kind) (.toString) (.substring 1))
               (if (utils/channel? id) 1 bucket-partition)
               encoded-id)
       v))))

(defn save-entries [entries]
  (doall (pmap save-entry entries)))

