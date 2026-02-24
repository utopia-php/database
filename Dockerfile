FROM composer:2 AS composer

WORKDIR /usr/local/src/

COPY composer.lock /usr/local/src/
COPY composer.json /usr/local/src/

RUN composer install \
    --ignore-platform-reqs \
    --optimize-autoloader \
    --no-plugins \
    --no-scripts \
    --prefer-dist

FROM appwrite/utopia-base:8.4-1.0.0 AS compile

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


## PCOV Extension
FROM compile AS pcov
RUN \
   git clone --depth 1 https://github.com/krakjoe/pcov.git \
   && cd pcov \
   && phpize \
   && ./configure --enable-pcov \
   && make && make install


FROM compile AS final

LABEL maintainer="team@appwrite.io"

ARG DEBUG=false
ENV DEBUG=$DEBUG

WORKDIR /usr/src/code

RUN echo extension=pcov.so >> /usr/local/etc/php/conf.d/pcov.ini
RUN echo extension=xdebug.so >> /usr/local/etc/php/conf.d/xdebug.ini

RUN mv "$PHP_INI_DIR/php.ini-production" "$PHP_INI_DIR/php.ini"

RUN echo "opcache.enable_cli=1" >> $PHP_INI_DIR/php.ini

RUN echo "memory_limit=1024M" >> $PHP_INI_DIR/php.ini

COPY --from=composer /usr/local/src/vendor /usr/src/code/vendor

COPY ./bin /usr/src/code/bin
COPY ./src /usr/src/code/src
COPY ./dev /usr/src/code/dev

# Add Debug Configs
RUN if [ "$DEBUG" = "true" ]; then cp /usr/src/code/dev/xdebug.ini /usr/local/etc/php/conf.d/xdebug.ini; fi
RUN if [ "$DEBUG" = "true" ]; then mkdir -p /tmp/xdebug; fi
RUN if [ "$DEBUG" = "false" ]; then rm -rf /usr/src/code/dev; fi
RUN if [ "$DEBUG" = "false" ]; then rm -f /usr/local/lib/php/extensions/no-debug-non-zts-20240924/xdebug.so; fi

CMD [ "tail", "-f", "/dev/null" ]
