FROM python:3.13-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /srv

# Dependencies first (cached layer), then the code.
COPY pyproject.toml ./
RUN pip install --upgrade pip && python -c "import tomllib; print('\n'.join(tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']))" > /tmp/req.txt && pip install -r /tmp/req.txt
COPY app ./app
RUN pip install --no-deps .

# Never run as root.
RUN useradd --create-home --uid 10001 inbox && mkdir -p /data && chown inbox /data
USER inbox
ENV DATABASE_URL=sqlite:////data/inbox.db

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health')"

# API by default; run the worker from the same image with:
#   docker run ... python -m app.worker
CMD ["uvicorn", "--factory", "app.main:create_app", "--host", "0.0.0.0", "--port", "8000"]
