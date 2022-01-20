FROM opencadc/astropy:3.9-slim

RUN apt-get update --no-install-recommends && apt-get dist-upgrade -y && \
    apt-get install -y \
        build-essential \
        git && \
    rm -rf /var/lib/apt/lists/ /tmp/* /var/tmp/*
    
RUN pip install cadcdata \
    cadctap \
    caom2 \
    caom2repo \
    caom2utils \
    importlib-metadata \
    python-dateutil \
    PyYAML \
    spherical-geometry \
    vos

WORKDIR /usr/src/app

ARG CAOM2_BRANCH=master
ARG CAOM2_REPO=opencadc
ARG OPENCADC_BRANCH=master
ARG OPENCADC_REPO=opencadc
ARG PIPE_BRANCH=master
ARG PIPE_REPO=opencadc
ARG VOS_BRANCH=master
ARG VOS_REPO=opencadc


RUN git clone https://github.com/opencadc/cadctools.git && \
    cd cadctools && \
    pip install ./cadcdata && \
    cd ..

RUN git clone https://github.com/${VOS_REPO}/vostools.git && \
    cd vostools && \
    git checkout ${VOS_BRANCH} && \
    pip install ./vos && \
    cd ..

RUN git clone https://github.com/${CAOM2_REPO}/caom2tools.git && \
    cd caom2tools && \
    git checkout ${CAOM2_BRANCH} && \
    pip install ./caom2utils && \
    cd ..


RUN pip install git+https://github.com/${OPENCADC_REPO}/caom2pipe@${OPENCADC_BRANCH}#egg=caom2pipe
  
RUN pip install git+https://github.com/${PIPE_REPO}/blank2caom2@${PIPE_BRANCH}#egg=blank2caom2


RUN useradd --create-home --shell /bin/bash cadcops
USER cadcops


ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
