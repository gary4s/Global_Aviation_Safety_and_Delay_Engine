# Use an optimized PySpark image
#FROM bitnami/spark:latest -- produced failed to resolve error
#The bitnami/spark:3.5 tag you’re looking for has likely been moved to their archive. 
#This is actually a perfect "Data Engineering" moment for your portfolio: dealing with breaking changes in upstream dependencies.
#The Fix: Switch to the Official Apache Image

FROM apache/spark:3.5.0-python3

USER root

# Install Azure CLI and remaining project dependencies
# Note: The official Spark image is based on Ubuntu/Debian, so these commands work
RUN apt-get update && apt-get install -y curl && \
    curl -sL https://aka.ms/InstallAzureCLIDeb | bash && \
    pip install --no-cache-dir \
    pandas \
    scikit-learn \
    mlflow \
    azure-storage-blob \
    azure-identity \
    azure-keyvault-secrets

WORKDIR /opt/spark/work-dir
EXPOSE 4040

CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]

