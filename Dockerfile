# Stage 1: Compile C++ binary for linux/amd64
FROM gcc:13-bookworm AS builder
WORKDIR /build
COPY cpp/main.cpp .
RUN g++ -O3 -DNDEBUG -march=native -mtune=native -flto -std=c++20 -pthread \
    -static-libstdc++ -static-libgcc -o pipeline main.cpp

# Stage 2: Runtime image
FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch runner

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pipeline/ /app/pipeline/
COPY --from=builder /build/pipeline /app/cpp/pipeline
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh /app/cpp/pipeline

USER 1000
CMD ["/app/run.sh"]
