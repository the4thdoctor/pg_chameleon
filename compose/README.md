# pg_chameleon development database container

A single Rocky Linux 10 image with everything needed to develop and test pg_chameleon:

- PostgreSQL 18 (PGDG repo)
- The [Sakila](https://dev.mysql.com/doc/sakila/en/) sample database, loaded into MariaDB on first start
- MariaDB (series set by the `MARIADB_VERSION` build arg, default 11.8), with ROW binlog enabled

Both servers run in the same container, started by `entrypoint.sh`.

## Files

| File | Purpose |
|------|---------|
| `Dockerfile` | Builds the image from `rockylinux/rockylinux:10` |
| `entrypoint.sh` | Initialises the data directories on first start, then runs both servers |
| `docker-compose.yml` | Service definition, ports, volumes, MariaDB binlog options |

## Quick start

```bash
cd compose
docker compose up --build -d
docker compose logs -f db
```

Connect:

```bash
psql -h 127.0.0.1 -U postgres            # password: postgres
mariadb -h 127.0.0.1 -uroot -pmariadb    # password: mariadb
```

## Entering the container

Open a shell in the running container:

```bash
docker compose exec db bash
# or, without compose
docker exec -it pgchameleon-db bash
```

Run a single command without opening a shell:

```bash
docker compose exec db psql -U postgres
docker compose exec db mariadb -uroot -pmariadb
docker compose exec -u postgres db psql -c "SELECT version()"
```

Useful once inside:

```bash
su - postgres -c "psql"              # PostgreSQL as the postgres OS user
mariadb -uroot -pmariadb             # MariaDB client
ls /var/lib/pgsql/18/data            # PostgreSQL data directory
ls /var/lib/mysql                    # MariaDB data directory
```

The `entrypoint.sh` process is PID 1, so the shell is a second process. Exiting it does not stop the container.

If the container fails to start (so `exec` is not possible), start a throwaway shell from the image instead, bypassing the entrypoint:

```bash
docker compose run --rm --entrypoint bash db
```

Stop (data is kept):

```bash
docker compose down
```

Stop and delete all data:

```bash
docker compose down -v
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_PASSWORD` | `postgres` | Password of the `postgres` superuser |
| `MARIADB_ROOT_PASSWORD` | `mariadb` | Password of `root@localhost` and `root@%` |
| `LOAD_SAKILA` | `1` | Load the `sakila` database into MariaDB on first start. Set to `0` to skip |

Both passwords are applied only when a data directory is first initialised. Changing them later has no effect on an existing volume. Change them with `ALTER USER` inside the database instead.

Build args (in `docker-compose.yml` under `build.args`):

| Arg | Default | Description |
|-----|---------|-------------|
| `PG_VERSION` | `18` | PostgreSQL major version |
| `MARIADB_VERSION` | `11.8` | MariaDB series, see <https://mariadb.org/download/> |

Extra arguments in `command:` are passed to `mariadbd`. The compose file sets:

```yaml
command: ["--log-bin=mysql-bin", "--binlog-format=ROW", "--binlog-row-image=FULL", "--server-id=1"]
```

These are the binlog settings pg_chameleon requires on the MySQL/MariaDB source.

## Sakila sample database

The image downloads Sakila from `downloads.mysql.com` at build time into `/usr/share/sakila`. On the first start of an empty MariaDB volume the entrypoint loads `sakila-schema.sql` and `sakila-data.sql`, so a `sakila` database (1000 films, 16044 rentals, plus views) is ready to replicate.

```bash
mariadb -h 127.0.0.1 -uroot -pmariadb -e "SELECT COUNT(*) FROM sakila.film"
```

Like the passwords, it is loaded only when the volume is first initialised. To reload it into an existing volume:

```bash
docker compose exec db bash -c \
  'mariadb -uroot -pmariadb < /usr/share/sakila/sakila-schema.sql &&
   mariadb -uroot -pmariadb < /usr/share/sakila/sakila-data.sql'
```

The schema script drops and recreates `sakila`, so any changes you made to it are lost.

## Persistent volumes

The image declares two volumes:

| Path in container | Contents |
|-------------------|----------|
| `/var/lib/pgsql` | PostgreSQL data (`PGDATA` is `/var/lib/pgsql/18/data`) |
| `/var/lib/mysql` | MariaDB data and binlogs |

The entrypoint only initialises a data directory when it is empty (no `PG_VERSION` file for PostgreSQL, no `mysql/` directory for MariaDB). A volume that already holds data is reused as it is.

### Option 1: named volumes (default)

This is what `docker-compose.yml` uses. Docker manages the storage:

```yaml
services:
  db:
    volumes:
      - pgdata:/var/lib/pgsql
      - mysqldata:/var/lib/mysql

volumes:
  pgdata:
  mysqldata:
```

Inspect or back up:

```bash
docker volume ls
docker volume inspect compose_pgdata
docker run --rm -v compose_pgdata:/src -v "$PWD":/backup rockylinux/rockylinux:10 \
    tar czf /backup/pgdata.tgz -C /src .
```

The volume names are prefixed with the compose project name (by default the directory name, `compose`).

### Option 2: bind mounts to host directories

Use this to keep the data at a known place on the host:

```yaml
services:
  db:
    volumes:
      - /srv/pg_chameleon/pgsql:/var/lib/pgsql
      - /srv/pg_chameleon/mysql:/var/lib/mysql
```

Create the directories first. The container's `postgres` and `mysql` users are created by the RPM packages, so their UIDs are not fixed. The entrypoint runs as root and `initdb` / `mariadb-install-db` are run as those users, so the simplest approach is to leave the host directories owned by root and let the entrypoint create the subdirectories:

```bash
sudo mkdir -p /srv/pg_chameleon/pgsql /srv/pg_chameleon/mysql
```

If you hit permission errors, check the UIDs with `docker compose exec db id postgres mysql` and `chown` the host directories to match.

On SELinux hosts (Rocky, RHEL, Fedora) add the `:Z` suffix so the container can write to the bind mount:

```yaml
    volumes:
      - /srv/pg_chameleon/pgsql:/var/lib/pgsql:Z
      - /srv/pg_chameleon/mysql:/var/lib/mysql:Z
```

### Option 3: named volume stored in a chosen host path

Keeps volume semantics but puts the data where you want:

```yaml
volumes:
  pgdata:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /srv/pg_chameleon/pgsql
  mysqldata:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /srv/pg_chameleon/mysql
```

The `device` directories must exist before `docker compose up`.

### Using plain `docker run`

```bash
docker build -t pg_chameleon-db:rocky10 .
docker run -d --name pgchameleon-db \
    -p 5432:5432 -p 3306:3306 \
    -v pgdata:/var/lib/pgsql \
    -v mysqldata:/var/lib/mysql \
    pg_chameleon-db:rocky10 \
    --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL --server-id=1
```

### Notes

- Keep PostgreSQL's data on its own volume from MariaDB's. The two paths above are separate on purpose, so one can be wiped without touching the other.
- PostgreSQL data is tied to the major version (`/var/lib/pgsql/18/data`). Changing `PG_VERSION` starts a fresh cluster instead of upgrading the old one. Use `pg_upgrade` or a dump/restore to move data between major versions.
- MariaDB binlogs live in `/var/lib/mysql`, so they persist with the data volume and grow over time. Add `--expire-logs-days=7` (or `--binlog-expire-logs-seconds`) to `command:` if disk usage matters.
- `docker compose down -v` removes named volumes. It does not touch bind-mounted host directories.

## Cleaning up and starting fresh

Pick the level you need. Every step from `down -v` onward deletes data.

**1. Stop the container, keep the data:**

```bash
docker compose down
```

**2. Stop and delete the named volumes** (PostgreSQL and MariaDB data, so the next start re-initialises both and reloads Sakila):

```bash
docker compose down -v
```

**3. Also delete the image**, to force a full rebuild:

```bash
docker compose down -v --rmi local
docker compose build --no-cache
```

**4. Full purge**: everything above plus orphan containers, the network and the build cache:

```bash
docker compose down -v --rmi all --remove-orphans
docker builder prune -f      # build cache (older Docker without buildx: docker image prune -f)
```

**Bind-mounted data** is not touched by `down -v`. Delete it by hand with the container stopped:

```bash
sudo rm -rf /srv/pg_chameleon/pgsql/* /srv/pg_chameleon/mysql/*
```

**Check that nothing is left:**

```bash
docker ps -a --filter name=pgchameleon
docker volume ls | grep -E 'pgdata|mysqldata'
docker images | grep pg_chameleon-db
```

**Remove a stray volume by name** (volumes are prefixed with the compose project name, `compose` by default):

```bash
docker volume rm compose_pgdata compose_mysqldata
```

**Docker-wide cleanup** (affects all projects, not only this one, so read the prompt first):

```bash
docker system prune -a --volumes
```

Then start again:

```bash
docker compose up --build -d
```

## Using it with pg_chameleon

Point the `sources` section of your pg_chameleon config at the MariaDB side and `pg_conn` at PostgreSQL:

```yaml
pg_conn:
  host: "127.0.0.1"
  port: "5432"
  user: "postgres"
  password: "postgres"

sources:
  mariadb:
    db_conn:
      host: "127.0.0.1"
      port: "3306"
      user: "root"
      password: "mariadb"
      charset: "utf8mb4"
```

## Status

The image has not been build-tested. The Rocky 10 repo URLs for PGDG and MariaDB are the most likely place for a build failure. Adjust them in the `Dockerfile` if so.

