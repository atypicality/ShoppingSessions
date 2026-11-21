FROM debian:bookworm-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg g++ \
    && curl -sSL https://apache.jfrog.io/artifactory/arrow/debian/apache-arrow-apt-source-latest-bookworm.deb -o arrow.deb \
    && apt-get install -y ./arrow.deb \
    && apt-get update && apt-get install -y --no-install-recommends libparquet-dev \
    && rm -rf /var/lib/apt/lists/* arrow.deb

WORKDIR /build
COPY cpp/main.cpp .
RUN g++ -O3 -std=c++20 -pthread main.cpp -o pipeline -lparquet -larrow

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && curl -sSL https://apache.jfrog.io/artifactory/arrow/debian/apache-arrow-apt-source-latest-bookworm.deb -o arrow.deb \
    && apt-get install -y ./arrow.deb \
    && apt-get update && apt-get install -y --no-install-recommends libparquet-glib-dev libparquet-dev \
    && apt-get purge -y curl gnupg && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* arrow.deb \
    && useradd --uid 1000 --no-create-home --home-dir /scratch runner

WORKDIR /app
COPY --from=builder /build/pipeline /app/pipeline
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh /app/pipeline

USER 1000
CMD ["/app/run.sh"]