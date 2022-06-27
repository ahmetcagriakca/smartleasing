# Python runtime with pyodbc to connect to SQL Server# Python runtime with pyodbc to connect to SQL Server
FROM  ahmetcagriakca/smart-leasing:base-1
RUN     mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN     pip3 install --upgrade pip
COPY    ./requirements.txt /usr/src/app/requirements.txt
RUN     pip3 install  -r requirements.txt

RUN     pip3 list
RUN     python3 --version
RUN     date

COPY    . /usr/src/app

# # openshift set permission to non-root users for /app directory
RUN chgrp -R 0 /usr/src/app && \
    chmod -R g=u /usr/src/app && \
    chgrp -R 0 /etc/passwd && \
    chmod -R g=u /etc/passwd

# # set running user 
USER 1001
ENTRYPOINT 	["/bin/sh"]
CMD 	["entrypoint.sh"]