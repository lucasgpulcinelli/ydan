FROM debian:bookworm AS build

WORKDIR /app

RUN apt-get update && \
  apt-get install -y openjdk-17-jdk-headless leiningen && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

COPY ./project.clj /app/

RUN lein deps

COPY ./ /app/

RUN lein uberjar


FROM debian:bookworm

WORKDIR /app

RUN apt-get update && \
  apt-get install -y openjdk-17-jre-headless && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

RUN useradd -M scraper

COPY --from=build --chmod=755 /app/target/uberjar/cljscrape-*-standalone.jar /app/cljscrape.jar

USER scraper

ENTRYPOINT ["java", "-jar", "cljscrape.jar"]
