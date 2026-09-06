FROM python:3.12-slim-bookworm

LABEL maintainer="Ege Gür"
LABEL description="Leaderless peer-to-peer async task mesh in Python"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install package dependencies
COPY pyproject.toml README.md /app/
RUN pip install --no-cache-dir .

# Copy application sources
COPY peerq /app/peerq
RUN pip install --no-cache-dir -e .

# Run as non-root user
RUN useradd -m -u 1000 -s /bin/bash peerq && \
    mkdir -p /app/data && \
    chown -R peerq:peerq /app

USER peerq

# Mesh TCP transport, HTTP status dashboard, and UDP discovery beacon
EXPOSE 9001 9102 7433/udp

ENTRYPOINT ["peerq"]
CMD ["--help"]
