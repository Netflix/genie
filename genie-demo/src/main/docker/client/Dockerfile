from alpine:3.4
MAINTAINER NetflixOSS <netflixoss@netflix.com>

RUN apk --no-cache add vim python py-pip python-dev bash g++ \
  && pip install --upgrade pip setuptools ipython pyyaml nflx-genie-client==3.0.48

COPY ./example/ /apps/genie/example/
WORKDIR /apps/genie/example
ENTRYPOINT ["/bin/bash"]
