# Runs examples/bench (see docker-compose.yml, topologies/docker_bridge.py). Build from
# the repo root:
#   docker compose -f examples/bench/docker-compose.yml build
#
# examples/bench never imports the vector_blf Rust extension, so this only needs the
# "testing" dependency group -- --no-install-project skips building vector-blf itself,
# so no Rust toolchain is needed in this image.
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --only-group testing --no-install-project

COPY examples/bench ./examples/bench

ENV PATH="/app/.venv/bin:${PATH}"
WORKDIR /app/examples/bench
ENTRYPOINT ["python", "main.py"]
CMD ["--topology", "quickstart", "--duration", "2"]
