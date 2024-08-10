FROM eclipse-temurin:8-jre-alpine
USER root
ENV SERVER_PORT=8449
EXPOSE ${SERVER_PORT}

# curl is used in the putTool script
# (PUT's can't be done using the busybox wget)
RUN apk --no-cache add curl

RUN mkdir -p /opt/aaihome/aaiadmin /opt/aai/logroot/AAI-GA /opt/app/aai-graphadmin/logs/gc

VOLUME /opt/aai/logroot/AAI-GA
VOLUME /opt/data
VOLUME /opt/tools

WORKDIR /opt/app/aai-graphadmin

RUN chown -R nobody:nobody /opt/app/aai-graphadmin /etc/profile.d /opt/aai/logroot/AAI-GA /opt/app /opt/aai/logroot /opt/app/aai-graphadmin/logs/gc

COPY --chown=nobody:nobody /maven/aai-graphadmin/ .

RUN find /opt/app/ -name "*.sh" -exec chmod +x {} + && \
    ln -s bin scripts && \
    ln -s /opt/aai/logroot/AAI-GA logs && \
    mv aai.sh /etc/profile.d/aai.sh && \
    chmod 755 /etc/profile.d/aai.sh

USER nobody
ENTRYPOINT ["/bin/sh", "/opt/app/aai-graphadmin/docker-entrypoint.sh"]
