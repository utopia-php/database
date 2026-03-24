FROM composer:2.8 AS composer

WORKDIR /usr/local/src/

COPY database/composer.lock /usr/local/src/
COPY database/composer.json /usr/local/src/

# Copy local dependencies (referenced as ../query and ../async in composer.json)
COPY query /usr/local/query
COPY async /usr/local/async

# Rewrite path repositories to use copied locations
RUN sed -i 's|"url": "../query"|"url": "/usr/local/query"|' /usr/local/src/composer.json \
 && sed -i 's|"url": "../async"|"url": "/usr/local/async"|' /usr/local/src/composer.json \
 && sed -i 's|"symlink": true|"symlink": false|' /usr/local/src/composer.json \
 && sed -i 's|"url": "../query"|"url": "/usr/local/query"|' /usr/local/src/composer.lock \
 && sed -i 's|"url": "../async"|"url": "/usr/local/async"|' /usr/local/src/composer.lock

RUN COMPOSER_MIRROR_PATH_REPOS=1 composer install \
    --ignore-platform-reqs \
    --optimize-autoloader \
    --no-plugins \
    --no-scripts \
    --prefer-dist

# Replace symlink with actual copy (composer path repos may still symlink)
RUN rm -rf /usr/local/src/vendor/utopia-php/query && \
    cp -r /usr/local/query /usr/local/src/vendor/utopia-php/query

FROM php:8.4.18-cli-alpine3.22 AS compile

ENV PHP_REDIS_VERSION="6.3.0" \
    PHP_SWOOLE_VERSION="v6.1.6" \
    PHP_XDEBUG_VERSION="3.4.2" \
    PHP_MONGODB_VERSION="2.1.1"
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apk update && apk add --no-cache \
    libpq \
    libpq-dev \
    make \
    automake \
    autoconf \
    gcc \
    g++ \
    git \
    brotli-dev \
    linux-headers \
    docker-cli \
    docker-cli-compose \
 && (pecl install mongodb-$PHP_MONGODB_VERSION \
    || (git clone --depth 1 --branch $PHP_MONGODB_VERSION --recurse-submodules https://github.com/mongodb/mongo-php-driver.git /tmp/mongodb \
      && cd /tmp/mongodb \
      && git submodule update --init --recursive \
      && phpize \
      && ./configure \
      && make \
      && make install \
      && cd / \
      && rm -rf /tmp/mongodb)) \
 && docker-php-ext-enable mongodb \
 && docker-php-ext-install opcache pgsql pdo_mysql pdo_pgsql \
 && apk del libpq-dev \
 && rm -rf /var/cache/apk/*

# Redis Extension
FROM compile AS redis
RUN \
  git clone --depth 1 --branch $PHP_REDIS_VERSION https://github.com/phpredis/phpredis.git \
  && cd phpredis \
  && phpize \
  && ./configure \
  && make && make install

## Swoole Extension
FROM compile AS swoole
RUN \
  git clone --depth 1 --branch $PHP_SWOOLE_VERSION https://github.com/swoole/swoole-src.git \
  && cd swoole-src \
  && phpize \
  && ./configure --enable-http2 \
  && make && make install

## PCOV Extension
FROM compile AS pcov
RUN \
   git clone --depth 1 https://github.com/krakjoe/pcov.git \
   && cd pcov \
   && phpize \
   && ./configure --enable-pcov \
   && make && make install

## XDebug Extension
FROM compile AS xdebug
RUN \
  git clone --depth 1 --branch $PHP_XDEBUG_VERSION https://github.com/xdebug/xdebug && \
  cd xdebug && \
  phpize && \
  ./configure && \
  make && make install

FROM compile AS final

LABEL maintainer="team@appwrite.io"

ARG DEBUG=false
ENV DEBUG=$DEBUG

WORKDIR /usr/src/code

RUN echo extension=redis.so >> /usr/local/etc/php/conf.d/redis.ini
RUN echo extension=swoole.so >> /usr/local/etc/php/conf.d/swoole.ini
RUN echo extension=pcov.so >> /usr/local/etc/php/conf.d/pcov.ini
RUN echo extension=xdebug.so >> /usr/local/etc/php/conf.d/xdebug.ini

RUN mv "$PHP_INI_DIR/php.ini-production" "$PHP_INI_DIR/php.ini"

RUN echo "opcache.enable_cli=1" >> $PHP_INI_DIR/php.ini

RUN echo "memory_limit=1024M" >> $PHP_INI_DIR/php.ini

COPY --from=composer /usr/local/src/vendor /usr/src/code/vendor
# Ensure local libs are copied (not symlinked) in vendor
COPY query /usr/src/code/vendor/utopia-php/query
COPY async /usr/src/code/vendor/utopia-php/async
COPY --from=swoole /usr/local/lib/php/extensions/no-debug-non-zts-20240924/swoole.so /usr/local/lib/php/extensions/no-debug-non-zts-20240924/
COPY --from=redis /usr/local/lib/php/extensions/no-debug-non-zts-20240924/redis.so /usr/local/lib/php/extensions/no-debug-non-zts-20240924/
COPY --from=pcov /usr/local/lib/php/extensions/no-debug-non-zts-20240924/pcov.so /usr/local/lib/php/extensions/no-debug-non-zts-20240924/
COPY --from=xdebug /usr/local/lib/php/extensions/no-debug-non-zts-20240924/xdebug.so /usr/local/lib/php/extensions/no-debug-non-zts-20240924/

COPY database/bin /usr/src/code/bin
COPY database/src /usr/src/code/src
COPY database/dev /usr/src/code/dev

# Add Debug Configs
RUN if [ "$DEBUG" = "true" ]; then cp /usr/src/code/dev/xdebug.ini /usr/local/etc/php/conf.d/xdebug.ini; fi
RUN if [ "$DEBUG" = "true" ]; then mkdir -p /tmp/xdebug; fi
RUN if [ "$DEBUG" = "false" ]; then rm -rf /usr/src/code/dev; fi
RUN if [ "$DEBUG" = "false" ]; then rm -f /usr/local/lib/php/extensions/no-debug-non-zts-20240924/xdebug.so; fi

CMD [ "tail", "-f", "/dev/null" ]
