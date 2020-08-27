FROM registry.access.redhat.com/ubi8

RUN yum install -y httpd mod_ssl

COPY frontend.conf /etc/httpd/conf.d/

RUN rm /etc/httpd/conf.d/welcome.conf
RUN sed -i \
    -e "s|^SSLCertificateFile .*$|SSLCertificateFile /etc/tls-secrets/tls.crt|" \
    -e "s|^SSLCertificateKeyFile .*$|SSLCertificateKeyFile /etc/tls-secrets/tls.key|" \
    -e "s|^ErrorLog .*|ErrorLog /dev/stderr|" \
    -e "s|^TransferLog .*|TransferLog /dev/stdout|" \
    /etc/httpd/conf.d/ssl.conf

CMD httpd -D FOREGROUND
