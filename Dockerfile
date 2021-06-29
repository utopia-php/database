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
ENV PHP_MONGO_VERSION=1.9.1

ENV PHP_REDIS_VERSION=5.3.4 \
    PHP_SWOOLE_VERSION=v4.6.7 \
    PHP_MONGO_VERSION=1.9.1
    
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN \
  apk update \
  && apk add --no-cache postgresql-libs postgresql-dev make automake autoconf gcc g++ git brotli-dev \
  && docker-php-ext-install opcache pgsql pdo_mysql pdo_pgsql \
  # Redis Extension
  && git clone --depth 1 --branch $PHP_REDIS_VERSION https://github.com/phpredis/phpredis.git \
  && cd phpredis \
  && phpize \
  && ./configure \
  && make && make install \
  && cd .. \
  ## Swoole Extension
  && git clone --depth 1 --branch $PHP_SWOOLE_VERSION https://github.com/swoole/swoole-src.git \
  && cd swoole-src \
  && phpize \
  && ./configure --enable-http2 \
  && make && make install \
  && cd .. \
  ## MongoDB Extension
  && git clone --depth 1 --branch $PHP_MONGO_VERSION https://github.com/mongodb/mongo-php-driver.git \
  && cd mongo-php-driver \
  && git submodule update --init \
  && phpize \
  && ./configure \
  && make && make install \
  && cd .. \
  && rm -rf /var/cache/apk/*

WORKDIR /usr/src/code

RUN echo extension=redis.so >> /usr/local/etc/php/conf.d/redis.ini
RUN echo extension=swoole.so >> /usr/local/etc/php/conf.d/swoole.ini
RUN echo extension=mongodb.so >> /usr/local/etc/php/conf.d/mongodb.ini

RUN mv "$PHP_INI_DIR/php.ini-production" "$PHP_INI_DIR/php.ini"

RUN echo "opcache.enable_cli=1" >> $PHP_INI_DIR/php.ini

RUN echo "memory_limit=1024M" >> $PHP_INI_DIR/php.ini

COPY --from=step0 /usr/local/src/vendor /usr/src/code/vendor

# Add Source Code
COPY . /usr/src/code

CMD [ "tail", "-f", "/dev/null" ]
