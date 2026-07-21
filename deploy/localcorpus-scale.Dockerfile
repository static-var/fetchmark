FROM scratch

COPY localcorpus-scale.test /localcorpus-scale.test

ENTRYPOINT ["/localcorpus-scale.test"]
