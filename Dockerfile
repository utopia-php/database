FROM composer:2.0 as step0

ARG TESTING=false
ENV TESTING=$TESTING

WORKDIR /usr/local/src/

COPY composer.lock /usr/local/src/
COPY composer.json /usr/local/src/

RUN composer update --ignore-platform-reqs --optimize-autoloader \
    --no-plugins --no-scripts --prefer-dist
    
FROM php:7.4-cli-alpine as final

LABEL maintainer="team@appwrite.io"

ENV PHP_SWOOLE_VERSION=v4.6.6
    
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN \
  apk update \
  && apk add --no-cache postgresql-libs postgresql-dev make automake autoconf gcc g++ git brotli-dev \
  && pecl install mongodb redis \ 
  && docker-php-ext-enable mongodb redis \
  && docker-php-ext-install opcache pgsql pdo_mysql pdo_pgsql \
  ## Swoole Extension
  && git clone --depth 1 --branch $PHP_SWOOLE_VERSION https://github.com/swoole/swoole-src.git \
  && cd swoole-src \
  && phpize \
  && ./configure --enable-http2 \
  && make && make install \
  && cd .. \
  && rm -rf /var/cache/apk/*

WORKDIR /usr/src/code

RUN echo extension=swoole.so >> /usr/local/etc/php/conf.d/swoole.ini

COPY --from=step0 /usr/local/src/vendor /usr/src/code/vendor

# Add Source Code
COPY ./ /usr/src/code

CMD [ "tail", "-f", "/dev/null" ]
