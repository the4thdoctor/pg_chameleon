#!/bin/bash
set -euo pipefail

: "${POSTGRES_PASSWORD:=postgres}"
: "${MARIADB_ROOT_PASSWORD:=mariadb}"
: "${LOAD_SAKILA:=1}"

# --- PostgreSQL ---
if [ ! -s "$PGDATA/PG_VERSION" ]; then
    install -d -o postgres -g postgres -m 700 "$PGDATA"
    su postgres -c "initdb -D $PGDATA --auth-local=trust --auth-host=scram-sha-256"
    echo "listen_addresses = '*'" >> "$PGDATA/postgresql.conf"
    echo "host all all 0.0.0.0/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"
    su postgres -c "pg_ctl -D $PGDATA -w start"
    su postgres -c "psql -v ON_ERROR_STOP=1 -c \"ALTER USER postgres PASSWORD '$POSTGRES_PASSWORD'\""
    su postgres -c "pg_ctl -D $PGDATA -w stop"
fi
su postgres -c "pg_ctl -D $PGDATA -w start"

# --- MariaDB ---
if [ ! -d /var/lib/mysql/mysql ]; then
    mariadb-install-db --user=mysql --datadir=/var/lib/mysql >/dev/null
    mariadbd --user=mysql --skip-networking &
    tmp=$!
    until mariadb-admin ping --silent 2>/dev/null; do sleep 1; done
    mariadb -e "DROP USER IF EXISTS ''@'localhost', ''@'$(hostname)'; \
                ALTER USER 'root'@'localhost' IDENTIFIED BY '$MARIADB_ROOT_PASSWORD'; \
                CREATE USER 'root'@'%' IDENTIFIED BY '$MARIADB_ROOT_PASSWORD'; \
                GRANT ALL ON *.* TO 'root'@'%' WITH GRANT OPTION;"
    if [ "$LOAD_SAKILA" = "1" ]; then
        mariadb -uroot -p"$MARIADB_ROOT_PASSWORD" < /usr/share/sakila/sakila-schema.sql
        mariadb -uroot -p"$MARIADB_ROOT_PASSWORD" < /usr/share/sakila/sakila-data.sql
    fi
    mariadb-admin -uroot -p"$MARIADB_ROOT_PASSWORD" shutdown
    wait "$tmp" || true
fi
mariadbd --user=mysql --bind-address=0.0.0.0 "$@" &
maria=$!

trap 'su postgres -c "pg_ctl -D $PGDATA -m fast stop"; kill $maria; wait $maria; exit 0' TERM INT
wait $maria
