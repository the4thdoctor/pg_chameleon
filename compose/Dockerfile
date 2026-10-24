FROM rockylinux/rockylinux:10

# Check https://mariadb.org/download/ and bump to the current stable series.
ARG MARIADB_VERSION=11.8
ARG PG_VERSION=18

RUN dnf -y install dnf-plugins-core epel-release \
 && dnf -y install \
      https://download.postgresql.org/pub/repos/yum/reporpms/EL-10-x86_64/pgdg-redhat-repo-latest.noarch.rpm \
 && dnf -qy module disable postgresql || true

RUN printf '%s\n' \
      '[mariadb]' \
      'name=MariaDB' \
      "baseurl=https://rpm.mariadb.org/${MARIADB_VERSION}/rhel/\$releasever/\$basearch" \
      'gpgkey=https://rpm.mariadb.org/RPM-GPG-KEY-MariaDB' \
      'gpgcheck=1' \
      'enabled=1' > /etc/yum.repos.d/mariadb.repo

RUN dnf -y install \
      postgresql${PG_VERSION}-server \
      postgresql${PG_VERSION}-contrib \
      MariaDB-server MariaDB-client \
      procps-ng hostname \
 && dnf clean all \
 # the RPM ships a pre-initialised datadir; empty it so entrypoint.sh initialises the volume
 && rm -rf /var/lib/mysql/*

# Sakila sample database, loaded into MariaDB on first start
RUN mkdir -p /usr/share/sakila \
 && curl -fsSL https://downloads.mysql.com/docs/sakila-db.tar.gz \
    | tar xz -C /usr/share/sakila --strip-components=1 \
 && ls /usr/share/sakila/sakila-schema.sql /usr/share/sakila/sakila-data.sql

ENV PGVERSION=${PG_VERSION} \
    PGDATA=/var/lib/pgsql/${PG_VERSION}/data \
    PATH=/usr/pgsql-${PG_VERSION}/bin:$PATH

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 5432 3306
VOLUME ["/var/lib/pgsql", "/var/lib/mysql"]
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
