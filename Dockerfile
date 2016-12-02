FROM python:3.5
MAINTAINER John Readey <jreadey@hdfgroup.org>
RUN pip install --upgrade pip                           ; \
    pip install numpy                                   ; \
    pip install aiobotocore                             ; \
    pip install pytz                                    ; \
    pip install requests                                ; \
    pip install psutil                                  ; \
    mkdir /usr/local/src/hsds
COPY hsds /usr/local/src/hsds
COPY entrypoint.sh  /

EXPOSE 5100-5999
 
ENTRYPOINT ["/entrypoint.sh"]