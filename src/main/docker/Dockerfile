FROM @aai.docker.namespace@/aai-common-@aai.base.image@:@aai.base.image.version@

RUN  mkdir -p /opt/aaihome/aaiadmin /opt/aai/logroot/AAI-GA /opt/app/aai-graphadmin/logs/gc

VOLUME /opt/aai/logroot/AAI-GA
VOLUME /opt/data
VOLUME /opt/tools

HEALTHCHECK --interval=40s --timeout=10s --retries=3 CMD nc -z -v localhost 8449 || exit 1

RUN groupadd aaiadmin -g 1000

RUN adduser -u 1000 -h /opt/aaihome/aaiadmin -S -D -G aaiadmin -s /bin/bash aaiadmin

# Add the proper files into the docker image from your build
WORKDIR /opt/app/aai-graphadmin

RUN chown -R aaiadmin:aaiadmin /opt/app/aai-graphadmin /etc/profile.d /opt/aai/logroot/AAI-GA /opt/app /opt/aai/logroot /opt/app/aai-graphadmin/logs/gc

COPY --chown=aaiadmin:aaiadmin /maven/aai-graphadmin/ .

USER aaiadmin

ENV AAI_BUILD_VERSION @aai.docker.version@
# Expose the ports for outside linux to use
# 8449 is the important one to be used
EXPOSE 8449
ENTRYPOINT ["/bin/bash", "/opt/app/aai-graphadmin/docker-entrypoint.sh"]