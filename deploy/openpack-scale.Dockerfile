FROM scratch

COPY openpack-scale.test /openpack-scale.test

ENTRYPOINT ["/openpack-scale.test"]
