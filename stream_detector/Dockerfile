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

COPY stream_detector/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# ---- runtime ---------------------------------------------------------------
FROM python:3.12-slim AS runtime

RUN groupadd --system app && useradd --system --gid app --home /app app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /etc/ssl/certs /etc/ssl/certs

WORKDIR /app
COPY --chown=app:app pipeline_common/ ./pipeline_common/
COPY --chown=app:app stream_detector/ ./stream_detector/

USER app

# No HTTP surface to probe -- it is a queue consumer -- so health is "can I
# still reach both dependencies?".
HEALTHCHECK --interval=15s --timeout=8s --start-period=20s --retries=5 \
    CMD ["python", "-m", "stream_detector.healthcheck"]

CMD ["python", "-m", "stream_detector.main"]
