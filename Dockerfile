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

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN \
  apk update \
  && apk add --no-cache postgresql-libs postgresql-dev make automake autoconf gcc g++ \
  && pecl install mongodb \ 
  && docker-php-ext-enable mongodb \
  && docker-php-ext-install opcache pgsql pdo_mysql pdo_pgsql \
  && rm -rf /var/cache/apk/*

WORKDIR /usr/src/code

COPY --from=step0 /usr/local/src/vendor /usr/src/code/vendor

# Add Source Code
COPY ./ /usr/src/code

CMD [ "tail", "-f", "/dev/null" ]
