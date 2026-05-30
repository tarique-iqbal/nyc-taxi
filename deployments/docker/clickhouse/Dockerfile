FROM clickhouse/clickhouse-server:24.4

# Server config
COPY config/config.xml /etc/clickhouse-server/config.d/custom.xml

# Users / profiles
COPY config/users.xml /etc/clickhouse-server/users.d/custom.xml

# Permissions
RUN chown clickhouse:clickhouse \
    /etc/clickhouse-server/config.d/custom.xml \
    /etc/clickhouse-server/users.d/custom.xml

# HTTP, Native TCP, Prometheus metrics
EXPOSE 8123 9000 9363

# Container health check
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD clickhouse-client --query "SELECT 1" || exit 1
