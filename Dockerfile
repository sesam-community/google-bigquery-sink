FROM ubuntu:20.04
MAINTAINER Mikkel Marensium Bakken "mikkel.bakken@sesam.io"

# Add locales package and set locale config
RUN \
apt-get update && \
DEBIAN_FRONTEND=noninteractive apt-get install -y locales && \
apt-get clean all && \
apt-get -y autoremove --purge && \
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN localedef -i en_US -f UTF-8 en_US.UTF-8
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
RUN locale-gen en_US.UTF-8
RUN DEBIAN_FRONTEND=noninteractive dpkg-reconfigure locales
ENV PYTHON_EGG_CACHE /tmp
ENV PYTHONIOENCODING UTF-8


# Add curl and some other base tools
RUN \
apt-get update && \
DEBIAN_FRONTEND=noninteractive apt-get install -y \
  apt-transport-https \
  apt-utils \
  curl \
  software-properties-common \
  build-essential \
  unzip \
  gosu \
  libsnappy1v5 \
  libgflags2.2 \
  tzdata \
  python3-dev \
  && \
apt-get clean all && \
apt-get -y autoremove --purge && \
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install pip
RUN curl --fail -sSL https://bootstrap.pypa.io/get-pip.py | python3 && python3 -m pip install pip

COPY ./service /service
WORKDIR /service
RUN pip install -r requirements.txt
EXPOSE 5001/tcp
ENTRYPOINT ["python3"]
CMD ["datasink-service.py"]
