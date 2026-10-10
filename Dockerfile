FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && JAVA_PATH="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
    && ln -sfn "${JAVA_PATH}" /opt/java-home

ENV JAVA_HOME=/opt/java-home

WORKDIR /workspace

COPY requirements.txt /tmp/requirements.txt
RUN pip install --requirement /tmp/requirements.txt

RUN useradd --create-home --uid 1000 --shell /bin/bash spark \
    && chown spark:spark /workspace

USER spark

EXPOSE 4040

CMD ["sleep", "infinity"]