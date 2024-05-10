FROM debian:12.5

RUN apt update && apt install -y vim libssl-dev curl libcurl4-openssl-dev libproj-dev libgdal-dev libudunits2-dev libxml2-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev librabbitmq-dev

### R
RUN apt install -y r-base r-base-dev r-cran-tidyverse r-cran-sf r-cran-dbscan r-cran-geosphere 

RUN R -e "install.packages('osmdata',dependencies=TRUE)"
RUN R -e "install.packages('leaflet.extras2',dependencies=TRUE)"
RUN R -e "install.packages('outliers')"
RUN R -e "install.packages('remotes')"
# for displaying the osm maps
RUN apt-get install -y firefox-esr

### python
RUN apt install -y python3 python3-pika python3-pandas

RUN useradd -u 1000 geo -g nogroup
USER geo
WORKDIR /home/geo

#CMD ["R"]
CMD ["sleep", "infinity"]
#CMD ["python3", "code/message_bus_consumer.py"]
#ENTRYPOINT ["tail", "-f", "/dev/null"]
