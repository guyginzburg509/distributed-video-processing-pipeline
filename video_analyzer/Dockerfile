# Build context is the repo root (see docker-compose.yml) so that the shared
# pipeline_common package is reachable from both service images.

# ---- builder ---------------------------------------------------------------
FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Optional: a PEM of extra trusted CAs, for networks behind a TLS-inspecting
# proxy or antivirus. Empty by default, so this is a no-op everywhere else.
ARG EXTRA_CA_BUNDLE=""
COPY docker/ca/ /tmp/ca/
RUN if [ -n "$EXTRA_CA_BUNDLE" ] && [ -f "/tmp/ca/$EXTRA_CA_BUNDLE" ]; then \
        cp "/tmp/ca/$EXTRA_CA_BUNDLE" /usr/local/share/ca-certificates/extra.crt && \
        apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
        update-ca-certificates && \
        rm -rf /var/lib/apt/lists/*; \
    fi
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY video_analyzer/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# ---- runtime ---------------------------------------------------------------
FROM python:3.12-slim AS runtime

# Non-root: an RCE in the app does not yield root inside the container, and the
# `restricted` Kubernetes Pod Security Standard requires it. Costs nothing here
# -- we bind port 8000, and the only mount is read-only.
RUN groupadd --system app && useradd --system --gid app --home /app app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /etc/ssl/certs /etc/ssl/certs

WORKDIR /app
COPY --chown=app:app pipeline_common/ ./pipeline_common/
COPY --chown=app:app video_analyzer/ ./video_analyzer/

USER app
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=5 \
    CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health', timeout=4).status == 200 else 1)"

# --no-access-log: structlog emits the structured request records we care about;
# uvicorn's plain-text access lines would only interleave a second format.
CMD ["uvicorn", "video_analyzer.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
