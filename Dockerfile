FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/workspace/src

RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Spark 3.5/Scala 2.12, its bundled Hadoop 3.3.4, and Iceberg 1.10.1.
# Hadoop's 3.3.4 parent POM pins aws-java-sdk-bundle to 1.12.262.
RUN python -c 'import pathlib, urllib.request; root=pathlib.Path("/usr/local/lib/python3.11/site-packages/pyspark/jars"); artifacts=[("org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1", "iceberg-spark-runtime-3.5_2.12-1.10.1.jar"), ("org/apache/hadoop/hadoop-aws/3.3.4", "hadoop-aws-3.3.4.jar"), ("com/amazonaws/aws-java-sdk-bundle/1.12.262", "aws-java-sdk-bundle-1.12.262.jar"), ("org/xerial/sqlite-jdbc/3.49.1.0", "sqlite-jdbc-3.49.1.0.jar")]; [(urllib.request.urlretrieve("https://repo.maven.apache.org/maven2/"+path+"/"+name, root/name)) for path,name in artifacts]'

COPY . ./

CMD ["python", "--version"]
