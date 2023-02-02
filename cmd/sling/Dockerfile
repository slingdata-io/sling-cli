FROM ubuntu:bionic

RUN apt update && apt install -y unzip alien libaio1 mysql-client-core-5.7 sshpass openssh-server postgresql-client wget curl && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists /var/cache/apt

# Install Oracle Instant Client
# from https://apextips.blogspot.com/2019/09/installing-oracle-instant-client-on.html
RUN cd /tmp && \
  wget https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm && \
  wget https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-devel-19.3.0.0.0-1.x86_64.rpm && \
  wget https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-sqlplus-19.3.0.0.0-1.x86_64.rpm && \
  wget https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-tools-19.3.0.0.0-1.x86_64.rpm && \
  alien -i oracle-instantclient19.3-*.rpm


RUN echo ' \
# Oracle Client environment
export ORACLE_HOME=/usr/lib/oracle/19.3/client64 \
export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH \
export PATH="$PATH:$ORACLE_HOME/bin" \
' >> /root/.bashrc

## Install mssql-tools
## from https://docs.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools?view=sql-server-ver15#ubuntu
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

RUN curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list | tee /etc/apt/sources.list.d/msprod.list

RUN export ACCEPT_EULA=y && apt-get update  && apt-get install -y mssql-tools unixodbc-dev && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists /var/cache/apt

RUN echo ' \
export PATH="$PATH:/opt/mssql-tools/bin"' >> /root/.bashrc

# Azure CLI
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash 

# SnowSQL
RUN cd /tmp && \
  curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.5-linux_x86_64.bash && \
  mkdir -p /usr/local/bin && \
  SNOWSQL_DEST=/usr/local/bin SNOWSQL_LOGIN_SHELL=/root/.bashrc bash snowsql-1.2.5-linux_x86_64.bash && \
  rm -f /tmp/snowsql-1.2.5-linux_x86_64.bash

# gcloud util
RUN cd /tmp && \
  wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-286.0.0-linux-x86_64.tar.gz && \
  tar -xf google-cloud-sdk-286.0.0-linux-x86_64.tar.gz && \
  ./google-cloud-sdk/install.sh --usage-reporting false --path-update true --quiet

ENV PATH="${PATH}:/usr/local/anaconda3/bin:/usr/local/go/bin:/root/go/bin:/code-server2:/opt/mssql-tools/bin:/usr/lib/oracle/19.3/client64/bin"

ENV SLING_PACKAGE="DOCKER"
ADD sling /usr/local/bin/sling
RUN chmod 755 /usr/local/bin/sling

ENTRYPOINT ["sling"]