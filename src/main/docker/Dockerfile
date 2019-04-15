FROM aaionap/aai-common:@aai.docker.version@-@aai.base.image@

RUN  mkdir -p /opt/aaihome/aaiadmin /opt/aai/logroot/AAI-GA

VOLUME /opt/aai/logroot/AAI-GA
VOLUME /opt/data
VOLUME /opt/tools

HEALTHCHECK --interval=40s --timeout=10s --retries=3 CMD nc -z -v localhost 8449 || exit 1

# Add the proper files into the docker image from your build
WORKDIR /opt/app/aai-graphadmin
COPY /maven/aai-graphadmin/ .

ENV AAI_BUILD_VERSION @aai.docker.version@
# Expose the ports for outside linux to use
# 8449 is the important one to be used
EXPOSE 8449
ENTRYPOINT ["/bin/bash", "/opt/app/aai-graphadmin/docker-entrypoint.sh"]