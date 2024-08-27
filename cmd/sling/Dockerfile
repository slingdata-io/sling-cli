FROM ubuntu:focal

RUN groupadd -r sling && useradd -r -g sling sling

RUN apt update && DEBIAN_FRONTEND=noninteractive apt install -y unzip libaio1 postgresql-client wget curl gnupg2 && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists /var/cache/apt

# Install Oracle Instant Client
RUN cd /tmp && \
  wget https://ocral.nyc3.cdn.digitaloceanspaces.com/sling/public/oracle_client64.tar.gz && \
  tar -xf oracle_client64.tar.gz && \
  mkdir -p /usr/lib/oracle/19.3 && mv oracle_client64 /usr/lib/oracle/19.3/client64 && \
  rm -f oracle_client64.tar.gz

## Install mssql-tools
## from https://docs.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools?view=sql-server-ver15#ubuntu
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

RUN curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | tee /etc/apt/sources.list.d/msprod.list

RUN export ACCEPT_EULA=y && apt-get update  && apt-get install -y mssql-tools unixodbc-dev && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists /var/cache/apt

COPY --chown=sling:sling sling /usr/local/bin/sling
RUN chmod 755 /usr/local/bin/sling

# Switch to non-root user
USER sling

# Final ENV updates
ENV ORACLE_HOME="/usr/lib/oracle/19.3/client64"
ENV LD_LIBRARY_PATH="/usr/lib/oracle/19.3/client64/lib"
ENV PATH="${PATH}:/usr/local/go/bin:/root/go/bin:/opt/mssql-tools/bin:/usr/lib/oracle/19.3/client64/bin"
ENV SLING_PACKAGE="DOCKER"

ENTRYPOINT ["sling"]