FROM debian:12.5

RUN apt update && apt install -y vim libssl-dev curl libcurl4-openssl-dev libproj-dev libgdal-dev libudunits2-dev libxml2-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev librabbitmq-dev

### R
RUN apt install -y r-base r-base-dev r-cran-tidyverse r-cran-sf r-cran-dbscan r-cran-geosphere r-cran-argparse r-cran-purrr

RUN R -e "install.packages('osmdata',dependencies=TRUE)"
RUN R -e "install.packages('leaflet.extras2',dependencies=TRUE)"
RUN R -e "install.packages('outliers')"
RUN R -e "install.packages('remotes')"

# for displaying the osm maps
RUN apt-get install -y firefox-esr

### python
RUN apt install -y python3 python3-pika python3-pandas

### user
RUN useradd -u 1000 geo -g nogroup

### R ATS_OPTICS package
RUN apt install -y texlive-latex-base # for pdflatex
ENV PATH_CODE /home/geo/code
COPY . ${PATH_CODE}
RUN cd ${PATH_CODE}/R/ATS_OPTICS && R -e "library(Rcpp); Rcpp.package.skeleton('ATSOPTICS',example_code=FALSE,code_files=c('ATS_OPTICS.R'),cpp_files=c('ATS_OPTICS.cpp'))"
RUN cd ${PATH_CODE}/R/ATS_OPTICS && R CMD build ATSOPTICS
RUN cd ${PATH_CODE}/R/ATS_OPTICS && R -e "install.packages('ATSOPTICS_1.0.tar.gz', repos = NULL, type='source')"

# change user and workdir
USER geo
WORKDIR /home/geo

#CMD ["sleep", "infinity"]
CMD ["python3", "code/message_bus_consumer.py"]
