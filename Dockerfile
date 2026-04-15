FROM python:3.11-slim AS base

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY static/ ./static/

# pytest suite image (non-root; no .env required — tests use temp dirs + mocks)
FROM base AS test
RUN groupadd tf2ls \
    && useradd -g tf2ls -u 10001 --no-create-home --home-dir /app tf2ls \
    && chown -R tf2ls:tf2ls /app
COPY --chown=tf2ls:tf2ls tests/ ./tests/
COPY --chown=tf2ls:tf2ls pytest.ini .
USER tf2ls
CMD ["python", "-m", "pytest", "-v", "tests"]

FROM base AS production

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
