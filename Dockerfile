FROM python:3.10.7-alpine3.16
USER root
RUN mkdir -p /src
WORKDIR /src
CMD ["export CACERT=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"]
CMD ["export TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)"]
CMD ["export APISERVER=https://kubernetes.default.svc.cluster.local"]
ADD requirements.txt .
RUN pip install -r requirements.txt
ADD . .
