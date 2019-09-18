FROM busybox:latest

COPY inetd.conf /etc/
#RUN apk add busybox-extras && which inetd
#&& inetd /etc/inetd.conf

RUN mkdir /data && for i in $(seq 1 10); do \
		fallocate -l 100M /data/$i.txt; done

#CMD busybox ftpd -w /
#CMD ifconfig | grep 172 && tcpsvd -v 0.0.0.0 21 ftpd -w -vv -a root /
#CMD ifconfig | grep 172 && busybox ftpd -wvv -a root /
CMD ifconfig | grep 172 && inetd -f /etc/inetd.conf



