# Utopia Database

[![Build Status](https://travis-ci.org/utopia-php/abuse.svg?branch=master)](https://travis-ci.com/utopia-php/database)
![Total Downloads](https://img.shields.io/packagist/dt/utopia-php/database.svg)
[![Discord](https://img.shields.io/discord/564160730845151244)](https://appwrite.io/discord)

Utopia framework database library is simple and lite library for managing application persistency using multiple database adapters. This library is aiming to be as simple and easy to learn and use. This library is maintained by the [Appwrite team](https://appwrite.io).

Although this library is part of the [Utopia Framework](https://github.com/utopia-php/framework) project it is dependency free, and can be used as standalone with any other PHP project or framework.

## Getting Started

Install using composer:
```bash
composer require utopia-php/database
```

Initialization:

```php
<?php

require_once __DIR__ . '/../../vendor/autoload.php';

```

## TODOS

- [x] Updated collection on deletion
- [x] Updated collection on attribute creation
- [x] Updated collection on attribute deletion
- [x] Updated collection on index creation
- [x] Updated collection on index deletion
- [ ] Validate original document before editing `$id`
- [x] Test duplicated document exception is being thrown
- [ ] Test no one can overwrite exciting documents/collections without permission
- [x] Add autorization validation layer
- [ ] Add strcture validation layer (based on new collection structure)
- [ ] Add cache layer (Delete / Update documents before cleaning cache)
- [ ] Implement find method
- [ ] Test for find timeout limits
- [ ] Merge new filter syntax parser

## System Requirements

Utopia Framework requires PHP 7.3 or later. We recommend using the latest PHP version whenever possible.

## Tests

To run all unit tests, use the following Docker command:

```bash
docker-compose exec tests vendor/bin/phpunit --configuration phpunit.xml tests
```

To run static code analysis, use the following Psalm command:

```bash
docker-compose exec tests vendor/bin/psalm --show-info=true
```
## Authors

**Eldad Fux**

+ [https://twitter.com/eldadfux](https://twitter.com/eldadfux)
+ [https://github.com/eldadfux](https://github.com/eldadfux)

**Brandon Leckemby**

+ [https://github.com/kodumbeats](https://github.com/kodumbeats)
+ [blog.leckemby.me](blog.leckemby.me)

## Copyright and license

The MIT License (MIT) [http://www.opensource.org/licenses/mit-license.php](http://www.opensource.org/licenses/mit-license.php)
