# NYC Dog-License DP POC — Tumult Analytics runs on Spark, which needs a JVM.
# We bundle a JRE so the host does NOT need Java installed (Decision 4).
#
# Base: eclipse-temurin:11-jre-jammy
#   - Temurin = official OpenJDK build from Adoptium, ships Java 11.
#   - Ubuntu 22.04 "jammy" ships Python 3.10 (same version as the earlier
#     python:3.10-slim base), so no extra Python toolchain setup.
#   - We deliberately use Java 11, not 17: PySpark 3.5 + Apache Arrow do not
#     work cleanly on Java 17 (Arrow's MemoryUtil needs sun.misc.Unsafe /
#     DirectByteBuffer, which Java 17 locks down).
#
# Force linux/amd64 because tmlt-core does not accept Linux aarch64 yet.
# (The warning "FromPlatformFlagConstDisallowed" is benign; the CLI
# --platform flag also sets it.)

FROM --platform=linux/amd64 eclipse-temurin:11-jre-jammy

# --- Python + minimal system packages ---------------------------------------
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev procps \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME / PATH are already set correctly by the Temurin base image.
# Sanity check at build time:
RUN java -version && python --version

WORKDIR /app

# --- Python deps (pins from Decision 4) --------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# --- App: single file, nothing more (Decision 4: stays minimal) --------------
COPY main.py .

# The CSV and .env are RUNTIME bind-mounts, not baked into the image.
#
#   docker build --platform linux/amd64 -t nyc-dog-dp .
#   docker run --rm --platform linux/amd64 \
#     -v "$PWD/data/nyc_dog_licenses.csv:/data/nyc_dog_licenses.csv:ro" \
#     -v "$PWD/.env:/app/.env:ro" \
#     nyc-dog-dp
#
CMD ["python", "main.py"]