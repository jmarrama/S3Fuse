# BUILD AS: kariusdx/go-alpine-fuse
FROM kariusdx/go-build:1.7.1-alpine

RUN apk update \
 && apk upgrade \
 && apk add yaml-dev curl-dev fuse-dev make g++ \
 && rm /var/cache/apk/* \
 && addgroup user \
 && adduser -G user -D user

ENV LIBFUSE_PATH=/usr/lib/libfuse.so.2

USER user
