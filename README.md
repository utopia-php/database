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

### Concepts

A list of the utopia/php concepts and their relevant equivalent using the different adapters

- **Database** - An instance of the utopia/database library that abstracts one of the supported adapters and provides a unified API for CRUD operation and queries on a specific schema or isolated scope inside the underlining database.
- ** Adapter** - An implementation of an underline database engine that this library can support, below is a list of [supported adapters](#supported-adapters) and supported capabilities for each Adapter.
- **Collection** - A set of documents stored on the same adapter scope. For SQL-based adapters, this will be equivalent to a table. For a No-SQL adapter, this will equivalent to a native collection.
- **Document** - A simple JSON object that will be stored in one of the utopia/database collections. For SQL-based adapters, this will be equivalent to a row. For a No-SQL adapter, this will equivalent to a native document.
- **Attribute** A simple document attribute. For SQL-based adapters, this will be equivalent to a column. For a No-SQL adapter, this will equivalent to a native document field.
- **Index** A simple collection index used to improve the performance of your database queries.

### Examples

Some examples to help you get started.

**Creating a database:**

```php
use PDO;
use Utopia\Database\Database;
use Utopia\Database\Adapter\MariaDB;

$dbHost = 'mariadb';
$dbPort = '3306';
$dbUser = 'root';
$dbPass = 'password';

$pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, [
    PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8mb4',
    PDO::ATTR_TIMEOUT => 3, // Seconds
    PDO::ATTR_PERSISTENT => true,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);

$database = new Database(new MariaDB($pdo));
$database->setNamespace('myscope');

$database->create(); // Creates a new schema named `myscope`
```

**Creating a collection:**

```php
$database->createCollection('documents');

// Add attributes
$database->createAttribute('documents', 'string', Database::VAR_STRING, 128);
$database->createAttribute('documents', 'integer', Database::VAR_INTEGER, 0);
$database->createAttribute('documents', 'float', Database::VAR_FLOAT, 0);
$database->createAttribute('documents', 'boolean', Database::VAR_BOOLEAN, 0);
$database->createAttribute('documents', 'colors', Database::VAR_STRING, 32, true,  true);

// Create an Index
$database->createIndex('indexes', 'index1', Database::INDEX_KEY, ['string', 'integer'], [128], [Database::ORDER_ASC]);
```

**Create a document:**

```php
$document = static::getDatabase()->createDocument('documents', new Document([
    '$read' => ['*', 'user1', 'user2'],
    '$write' => ['*', 'user1', 'user2'],
    'string' => 'text📝',
    'integer' => 5,
    'float' => 5.55,
    'boolean' => true,
    'colors' => ['pink', 'green', 'blue'],
]));
```

### Adapters

Below is a list of supported adapters, and thier compatibly tested versions alongside a list of supported features and relevant limits.

| Adapter | Status | Version |
|---------|---------|---|
| MariaDB | ✅ | 10.5 |
| MySQL | ✅ | 8.0 |
| Postgres | 🛠 | 13.0 |
| MongoDB | 🛠 | 3.6 |
| SQLlite | 🛠 | 3.35 |

` ✅  - supported, 🛠  - work in progress`

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
