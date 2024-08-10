FROM ubuntu:focal

RUN apt update && DEBIAN_FRONTEND=noninteractive apt install -y unzip alien libaio1 postgresql-client wget curl && \
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

RUN curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | tee /etc/apt/sources.list.d/msprod.list

RUN export ACCEPT_EULA=y && apt-get update  && apt-get install -y mssql-tools unixodbc-dev && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists /var/cache/apt

RUN echo ' \
export PATH="$PATH:/opt/mssql-tools/bin"' >> /root/.bashrc

ENV PATH="${PATH}:/usr/local/go/bin:/root/go/bin:/opt/mssql-tools/bin:/usr/lib/oracle/19.3/client64/bin"

ENV SLING_PACKAGE="DOCKER"
ADD sling /usr/local/bin/sling
RUN chmod 755 /usr/local/bin/sling

ENTRYPOINT ["sling"]