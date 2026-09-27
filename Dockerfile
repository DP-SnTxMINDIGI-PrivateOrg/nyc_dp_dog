# NYC Dog-License DP POC — Tumult Analytics runs on Spark, which needs a JVM.
# We bundle a headless JDK so the host does NOT need Java installed (Decision 4).

FROM python:3.10-slim

# --- Java (Decision 4): default-jre-headless is small, no GUI needed ---------
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# PySpark discovers the JVM via JAVA_HOME. On Debian, default-java points at it.
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# --- Python deps (pins from Decision 4) --------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- App: single file, nothing more (Decision 4: stays minimal) --------------
COPY main.py .

# The CSV and .env are RUNTIME bind-mounts, not baked into the image, so the
# same image serves any ZIP/breed/borough combination without a rebuild:
#
#   docker build -t nyc-dog-dp .
#   docker run --rm \
#     -v "$PWD/nyc_dog_licenses.csv:/data/nyc_dog_licenses.csv:ro" \
#     -v "$PWD/.env:/app/.env:ro" \
#     nyc-dog-dp
#
CMD ["python", "main.py"]