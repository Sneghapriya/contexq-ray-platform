# Official Ray image: same base KubeRay uses, so versions line up.
# Set RAY_VERSION to match requirements.txt exactly.
ARG RAY_VERSION=2.59.0
FROM rayproject/ray:${RAY_VERSION}-py311

WORKDIR /home/ray/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY data/ ./data/

# The pipeline reads these at runtime; the cluster overrides them later
ENV DATA_DIR=/home/ray/app/data
ENV ICEBERG_WAREHOUSE=/tmp/warehouse