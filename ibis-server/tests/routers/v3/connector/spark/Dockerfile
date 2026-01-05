FROM apache/spark:3.5.7

USER root

# Create warehouse and work directories with proper permissions
RUN mkdir -p /opt/spark/work/warehouse && \
    chown -R spark:spark /opt/spark/work && \
    chmod -R 777 /opt/spark/work

USER spark