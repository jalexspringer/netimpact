FROM python:3.8-slim-buster
RUN pip install netimpact

ENTRYPOINT [ "netimpact", "-tp"]
CMD ["awin,ls,admitad"]