# Utopia Database

[![Build Status](https://travis-ci.org/utopia-php/database.svg?branch=master)](https://travis-ci.com/utopia-php/database)
![Total Downloads](https://img.shields.io/packagist/dt/utopia-php/database.svg)
[![Discord](https://img.shields.io/discord/564160730845151244?label=discord)](https://appwrite.io/discord)

Utopia framework database library is simple and lite library for managing application persistency using multiple database adapters. This library is aiming to be as simple and easy to learn and use. This library is maintained by the [Appwrite team](https://appwrite.io).

Although this library is part of the [Utopia Framework](https://github.com/utopia-php/framework) project it is dependency free, and can be used as standalone with any other PHP project or framework.

Upgrading from 7.x? Read [UPGRADE.md](UPGRADE.md). [CHANGELOG.md](CHANGELOG.md) lists what is new in each release.

## Getting Started

Install using composer:

```bash
composer require utopia-php/database
```

### Concepts

A list of the utopia/php concepts and their relevant equivalent using the different adapters

- **Database** - An instance of the utopia/database library that abstracts one of the supported adapters and provides a unified API for CRUD operation and queries on a specific schema or isolated scope inside the underlining database.
- **Adapter** - An implementation of an underlying database engine that this library can support - below is a list of [supported databases](#supported-databases) and supported capabilities for each Database.
- **Collection** - A set of documents stored on the same adapter scope. For SQL-based adapters, this will be equivalent to a table. For a No-SQL adapter, this will equivalent to a native collection.
- **Document** - A simple JSON object that will be stored in one of the utopia/database collections. For SQL-based adapters, this will be equivalent to a row. For a No-SQL adapter, this will equivalent to a native document.
- **Attribute** - A simple document attribute. For SQL-based adapters, this will be equivalent to a column. For a No-SQL adapter, this will equivalent to a native document field.
- **Index** - A simple collection index used to improve the performance of your database queries.
- **Permissions** - Using permissions, you can decide which roles have read, create, update and delete access for a specific document. The special attribute `$permissions` is used to store permission metadata for each document in the collection. A permission role can be any string you want. You can use `$database->getAuthorization()->addRole()` to delegate new roles to your users, once obtained a new role a user would gain read, create, update or delete access to a relevant document.
- **Hooks** - Objects registered with `$database->addHook()` that take part in database operations: document permissions and relationships are hooks, and so are your own event listeners. See [Hooks](#hooks).

### Filters

Attribute filters are functions that manipulate attributes before saving them to the database and after retrieving them from the database. You can add filters using the `Database::addFilter($name, $encode, $decode)` where `$name` is the name of the filter that we can add later to attribute `filters` array. `$encode` and `$decode` are the functions used to encode and decode the attribute, respectively. Filters added with `Database::addFilter()` apply to every `Database` instance in the process. There are also instance-level filters that can only be defined while constructing the `Database` instance, and custom types registered on a `Utopia\Database\Type\TypeRegistry` that apply to the instances you give the registry to with `setTypeRegistry()`. Instance level filters override the static filters if they have the same name.

### Custom Document Types

The database library supports mapping custom document classes to specific collections, enabling a domain-driven design approach. This allows you to create collection-specific classes (like `User`, `Post`, `Product`) that extend the base `Document` class with custom methods and business logic.

```php
use Utopia\Database\Document;

// Define a custom document class
class User extends Document
{
    public function getEmail(): string
    {
        return $this->getAttribute('email', '');
    }

    public function isAdmin(): bool
    {
        return $this->getAttribute('role') === 'admin';
    }
}

// Register the custom type
$database->setDocumentType('users', User::class);

// Now all documents from 'users' collection are User instances
$user = $database->getDocument('users', 'user123');
$email = $user->getEmail(); // Use custom methods
if ($user->isAdmin()) {
    // Domain logic
}
```

**Benefits:**
- ✅ Domain-driven design with business logic in domain objects
- ✅ Type safety with IDE autocomplete for custom methods
- ✅ Code organization and encapsulation
- ✅ Fully backwards compatible

### Reserved Attributes

- `$id` - the document unique ID, you can set your own custom ID or a random UID will be generated by the library.
- `$sequence` - the document's internal sequence number, set by the database when the document is created.
- `$createdAt` - the document creation date, this attribute is automatically set when the document is created.
- `$updatedAt` - the document update date, this attribute is automatically set when the document is updated.
- `$collection` - an attribute containing the name of the collection the document is stored in.
- `$permissions` - an attribute containing an array of strings. Each string represent a specific action and role. If your user obtains that role for that action they will have access for this document.
- `$tenant` - the tenant a document belongs to, when collections are shared between tenants (`setSharedTables(true)`).

### Attribute Types

Attribute types are cases of `Utopia\Query\Schema\ColumnType`: `String`, `Varchar`, `Text`, `MediumText`, `LongText`, `Integer`, `BigInteger`, `Float`, `Double`, `Boolean`, `Datetime`, `Id`, `Relationship`, `Object`, `Point`, `Linestring`, `Polygon` and `Vector`. `Attribute::TYPES` lists them, and object, spatial and vector attributes need an adapter that supports them. Attributes of the other types can hold an array of values (`array: true`). Arrays and objects are encoded to JSON when stored and decoded back when fetched, where the adapter has no native type for them.

### Supported Databases

Below is a list of supported databases, and their compatibly tested versions alongside a list of supported features and relevant limits.

| Adapter  | Status | Version |
|----------|--------|---------|
| MariaDB  | ✅      | 10.11   |
| MySQL    | ✅      | 8.0     |
| Postgres | ✅      | 16      |
| SQLite   | ✅      | 3.38    |
| MongoDB  | ✅      | 8.0     |
| Redis    | ✅      | 8.2     |
| Memory   | ✅      | -       |

` ✅  - supported `

` 🛠  - work in progress`

What an adapter supports is reported by `$database->getAdapter()->supports(Capability::...)` and `$database->getAdapter()->hasFeature(Feature\...::class)`. Joins and aggregations, for example, run on the SQL adapters.

### Limitations 

#### MariaDB, MySQL, Postgres, SQLite
- ID max size can be 255 bytes
- ID can only contain [^A-Za-z0-9] and symbols `_` `-`
- Document max size is 65535 bytes
- Collection can have a max of 1017 attributes
- Collection can have a max of 64 indexes
- Index value max size is 768 bytes. Values over 768 bytes are truncated
- String max size is 4294967295 characters
- Integer max size is 4294967295 

#### MongoDB
- ID max size can be 255 bytes
- ID can only contain [^A-Za-z0-9] and symbols `_` `-`
- Document can have unrestricted size
- Collection can have unrestricted amount of attributes 
- Collection can have a max of 64 indexes
- Index value can have unrestricted size
- String max size is 2147483647 characters 
- Integer max size is 4294967295 

## Usage

### Connecting to a Database 

`Utopia\Database\PDO` wraps PHP's PDO: it reconnects when a connection is lost outside a transaction and retries the call. Each SQL adapter also accepts a plain `PDO`.

#### MariaDB

```php
require_once __DIR__ . '/vendor/autoload.php';

use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Database;
use Utopia\Database\PDO;

$dbHost = 'mariadb';
$dbPort = '3306';
$dbUser = 'root';
$dbPass = 'password';

$pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MariaDB::getPDOAttributes());

$cache = new Cache(new Memory()); // or use any cache adapter you wish

$database = new Database(new MariaDB($pdo), $cache);
```

#### MySQL

```php
require_once __DIR__ . '/vendor/autoload.php';

use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Database;
use Utopia\Database\PDO;

$dbHost = 'mysql';
$dbPort = '3306';
$dbUser = 'root';
$dbPass = 'password';

$pdo = new PDO("mysql:host={$dbHost};port={$dbPort};charset=utf8mb4", $dbUser, $dbPass, MySQL::getPDOAttributes());

$cache = new Cache(new Memory()); // or use any cache adapter you wish

$database = new Database(new MySQL($pdo), $cache);
```

#### Postgres

```php
require_once __DIR__ . '/vendor/autoload.php';

use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Database;
use Utopia\Database\PDO;

$dbHost = 'postgres';
$dbPort = '5432';
$dbUser = 'root';
$dbPass = 'password';

$pdo = new PDO("pgsql:host={$dbHost};port={$dbPort}", $dbUser, $dbPass, Postgres::getPDOAttributes());

$cache = new Cache(new Memory()); // or use any cache adapter you wish

$database = new Database(new Postgres($pdo), $cache);
```

#### SQLite

```php
require_once __DIR__ . '/vendor/autoload.php';

use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\PDO;

$dbPath = '/path/to/database.sqlite';

$pdo = new PDO("sqlite:{$dbPath}", null, null, SQLite::getPDOAttributes());

$cache = new Cache(new Memory()); // or use any cache adapter you wish

$database = new Database(new SQLite($pdo), $cache);
```

#### MongoDB

```php
require_once __DIR__ . '/vendor/autoload.php';

use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Database;
use Utopia\Mongo\Client; // from utopia-php/mongo

$dbHost = 'mongo';
$dbPort = 27017;
$dbUser = 'root';
$dbPass = 'password';
$dbName = 'dbName';

$mongoClient = new Client($dbName, $dbHost, $dbPort, $dbUser, $dbPass, true);

$cache = new Cache(new Memory()); // or use any cache adapter you wish

$database = new Database(new Mongo($mongoClient), $cache);
```

### Hooks

Document permissions and relationships are hooks. Register both after you create the `Database`:

```php
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;

// Stores the rows that document permissions are checked against on the SQL adapters
$database->addHook(new Permissions());

// Populates related documents and handles nested writes and cascades
$database->addHook(new Relationships($database));
```

To act on database events, register a lifecycle hook. It receives every event (`Utopia\Database\Event`), so check the event inside `handle()`. A hook that also implements `Named` replaces the hook already registered under its name, and `silent()` can silence it by name.

```php
use Utopia\Database\Event;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Named;

final class AuditLog implements Lifecycle, Named
{
    /** @var array<string> */
    public array $entries = [];

    public function getName(): string
    {
        return 'audit-log';
    }

    public function handle(Event $event, mixed $data): void
    {
        if ($event === Event::DocumentCreate || $event === Event::DocumentDelete) {
            $this->entries[] = $event->value;
        }
    }
}

$auditLog = new AuditLog();
$database->addHook($auditLog);

// Run a callback without the named hooks, or without any hook when no names are given
$database->silent(fn () => $database->ping(), ['audit-log']);
```

A `Utopia\Database\Hook\Transform` rewrites SQL statements before they run, and a `Utopia\Database\Hook\Decorator` modifies the documents that reads and writes return. Both are registered with `addHook()` too.

### Database Methods

```php
// Get namespace
$database->getNamespace();

// Sets namespace that prefixes all collection names
$database->setNamespace(
    namespace: 'namespace'
);

// Get default database
$database->getDatabase();

// Sets default database
$database->setDatabase(
    name: 'dbName'
);

// Check if a database exists
if ($database->exists(database: 'dbName')) {
    // Delete a database
    $database->delete(
        database: 'dbName'
    );
}

// Creates a new database.
// Uses default database as the name.
$database->create();

// Returns an array of all databases
$database->list();

// Check if collection exists
$database->exists(
    database: 'dbName',
    collection: 'users'
);

// Ping database it returns true if the database is alive
$database->ping();

// Get Database Adapter
$database->getAdapter();

// Get List of keywords that cannot be used
$database->getKeywords();
```

### Collection Methods

```php
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Query\Schema\Order;

// Creates a new collection named 'users'. The SQL adapters store it as the table '$namespace_users',
// with the table '$namespace_users_perms' for its document permissions.
$database->createCollection(new Collection(
    id: 'users',
    attributes: [
        Attribute::string(key: 'name', size: 256),
        Attribute::integer(key: 'age'),
    ],
    indexes: [
        Index::key(key: 'idx_name', attributes: ['name'], lengths: [256], orders: [Order::Asc]),
        Index::key(key: 'idx_name_age', attributes: ['name', 'age'], lengths: [128, null], orders: [Order::Asc, Order::Desc]),
    ],
    permissions: [
        Permission::create(Role::any()),
        Permission::read(Role::any()),
    ],
    documentSecurity: true,
));

// Update Collection Permissions
$database->updateCollection(
    id: 'users',
    permissions: [
        Permission::create(Role::any()),
        Permission::read(Role::any()),
        Permission::update(Role::any()),
        Permission::delete(Role::any())
    ],
    documentSecurity: true
);

// Get Collection
$database->getCollection(
    id: 'users'
);

// List Collections
$database->listCollections(
    limit: 25,
    offset: 0
);

// Delete cached documents of a collection
$database->purgeCachedCollection(
    collectionId: 'users'
);

// Deletes the collection and its permissions table
$database->createCollection(new Collection(id: 'drafts'));
$database->deleteCollection(
    id: 'drafts'
);
```

### Attribute Methods

```php
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;
use Utopia\Validator\Range;

$database->createCollection(new Collection(
    id: 'movies',
    permissions: [
        Permission::create(Role::any()),
        Permission::read(Role::any()),
        Permission::update(Role::any()),
        Permission::delete(Role::any()),
    ],
));

// Creates a new attribute named 'name' in the 'movies' collection.
// Every type has a factory: Attribute::string(), integer(), float(), boolean(), datetime(), ...
$database->createAttribute('movies', Attribute::string(
    key: 'name',
    size: 128,
    required: true
));

// New attribute with optional parameters
$database->createAttribute('movies', Attribute::string(
    key: 'genres',
    size: 128,
    required: false,
    default: null,
    signed: true,
    array: true,
    format: null,
    formatOptions: [],
    filters: []
));

// The same with the model's constructor
$database->createAttribute('movies', new Attribute(
    key: 'director',
    type: ColumnType::String,
    size: 128
));

// Creates several attributes at once
$database->createAttributes('movies', [
    Attribute::integer(key: 'year'),
    Attribute::float(key: 'price'),
    Attribute::boolean(key: 'active'),
]);

// Updates the attribute named 'genres' in the 'movies' collection.
$database->updateAttribute(
    collection: 'movies',
    id: 'genres',
    type: ColumnType::String,
    size: 128,
    required: false,
    default: null,
    signed: true,
    array: true,
    format: null,
    formatOptions: [],
    filters: []
);

// Update the required status of an attribute
$database->updateAttributeRequired(
    collection: 'movies',
    id: 'genres',
    required: true
);

// Update the attribute format. A format is a validator registered for an attribute type.
Structure::addFormat(
    'year',
    fn (array $attribute) => new Range($attribute['formatOptions']['min'] ?? 0, $attribute['formatOptions']['max'] ?? 9999),
    ColumnType::Integer
);

$database->updateAttributeFormat(
    collection: 'movies',
    id: 'year',
    format: 'year'
);

// Update the attribute format options
$database->updateAttributeFormatOptions(
    collection: 'movies',
    id: 'year',
    formatOptions: ['min' => 1888, 'max' => 2100]
);

// Update the attribute filters
$database->updateAttributeFilters(
    collection: 'movies',
    id: 'genres',
    filters: []
);

// Update the default value of an attribute
$database->updateAttributeDefault(
    collection: 'movies',
    id: 'director',
    default: 'Unknown'
);

// Check if attribute can be added to a collection
$collection = $database->getCollection('movies');

$database->checkAttribute(
    collection: $collection,
    attribute: Attribute::integer(key: 'rating')
);

// Get Adapter attribute limit
$database->getLimitForAttributes(); // if 0 then no limit

// Get Adapter index limit
$database->getLimitForIndexes();

// Renames the attribute from old to new in the 'movies' collection.
$database->createAttribute('movies', Attribute::string(key: 'tagline', size: 256));
$database->renameAttribute(
    collection: 'movies',
    old: 'tagline',
    new: 'slogan'
);

// Deletes the attribute in the 'movies' collection.
$database->deleteAttribute(
    collection: 'movies',
    id: 'slogan'
);
```

### Index Methods

```php
use Utopia\Database\Index;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\Order;

// Index types are cases of IndexType: Key, Unique, Fulltext, Spatial, Object, Trigram, Ttl,
// HnswEuclidean, HnswCosine and HnswDot. Every type has a factory: Index::key(), unique(), fullText(), ...
// Orders are Order::Asc and Order::Desc.

// Creates a new index named 'index1' in the 'movies' collection.
$database->createIndex('movies', Index::key(
    key: 'index1',
    attributes: ['name', 'year'],
    lengths: [128, null],
    orders: [Order::Asc, Order::Desc]
));

// The same with the model's constructor
$database->createIndex('movies', new Index(
    key: 'index_name_search',
    type: IndexType::Fulltext,
    attributes: ['name']
));

// Rename index from old to new in the 'movies' collection.
$database->renameIndex(
    collection: 'movies',
    old: 'index1',
    new: 'index2'
);

// Deletes the index in the 'movies' collection.
$database->deleteIndex(
    collection: 'movies',
    id: 'index2'
);
```

### Relationship Methods

```php
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ForeignKeyAction;

// Relationship types are cases of RelationType: OneToOne, OneToMany, ManyToOne and ManyToMany.
// What happens to related documents when a document is deleted is a ForeignKeyAction:
// Restrict (the default), Cascade or SetNull.

// Creates a relationship between the two collections with the default reference attributes:
// 'users' on 'movies', and 'movies' on 'users'
$database->createRelationship(new Relationship(
    collection: 'movies',
    relatedCollection: 'users',
    type: RelationType::OneToOne,
    twoWay: true
));

// Create a relationship with custom reference attributes. Every type has a factory.
$database->createCollection(new Collection(
    id: 'reviews',
    attributes: [Attribute::string(key: 'body', size: 1024)],
    permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
));

$database->createRelationship(Relationship::oneToMany(
    collection: 'movies',
    relatedCollection: 'reviews',
    twoWay: true,
    key: 'reviews',
    twoWayKey: 'movie',
    onDelete: ForeignKeyAction::Cascade
));

// Update the relationship with the default reference attributes
$database->updateRelationship(
    collection: 'movies',
    id: 'users',
    onDelete: ForeignKeyAction::SetNull
);

// Update the relationship with custom reference attributes
$database->updateRelationship(
    collection: 'movies',
    id: 'users',
    newKey: 'viewer',
    newTwoWayKey: 'favoriteMovie',
    twoWay: true
);

// Delete the relationship with the default or custom reference attributes
$database->deleteRelationship(
    collection: 'movies',
    id: 'viewer'
);
```

### Document Methods

```php
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\SetType;

// Id helpers
ID::unique(padding: 12); // Creates an id of 13 characters from uniqid() followed by 12 random hex characters
ID::custom(id: 'my_user_3235');

// Role helpers
Role::any();
Role::guests();
Role::user(
    identifier: ID::unique(),
    status: 'verified' // optional
);
Role::users();
Role::team(
    identifier: ID::unique()
);
Role::team(
    identifier: ID::unique(),
    dimension: '123' // team:id/dimension
);
Role::label(
    identifier: 'admin'
);
Role::member(
    identifier: ID::unique()
);

// Permission helpers
Permission::read(Role::any());
Permission::create(Role::user(ID::unique()));
Permission::update(Role::user(ID::unique(padding: 23)));
Permission::delete(Role::user(ID::custom(id: 'my_user_3235')));

// To create a document
$document = new Document([
    '$permissions' => [
        Permission::read(Role::any()),
        Permission::update(Role::user(ID::custom('1x'))),
        Permission::delete(Role::user(ID::unique(12))),
    ],
    '$id' => ID::unique(),
    'name' => 'Captain Marvel',
    'director' => 'Anna Boden & Ryan Fleck',
    'year' => 2019,
    'price' => 25.99,
    'active' => true,
    'genres' => ['science fiction', 'action', 'comics'],
]);

$document = $database->createDocument(
    collection: 'movies',
    document: $document
);

// Get which collection a document belongs to
$document->getCollection();

// Get document id
$document->getId();

// Check whether document in empty
$document->isEmpty();

// Increase an attribute in a document
$database->increaseDocumentAttribute(
    collection: 'movies',
    id: $document->getId(),
    attribute: 'price',
    value: 5,
    max: 100
);

// Decrease an attribute in a document
$database->decreaseDocumentAttribute(
    collection: 'movies',
    id: $document->getId(),
    attribute: 'price',
    value: 5,
    min: 0
);

// Update the value of an attribute in a document

// Set types are cases of SetType:
// SetType::Assign assigns the new value directly (the default),
// SetType::Append appends the new value to the end of an array,
// SetType::Prepend prepends the new value to the start of an array.
// Appending or prepending to an attribute that is not an array sets it to an array containing the new value.
$document->setAttribute('name', 'Captain Marvel (2019)')
         ->setAttribute('genres', 'superhero', SetType::Append);

$document = $database->updateDocument(
    collection: 'movies',
    id: $document->getId(),
    document: $document
);

// Update the permissions of a document
$document->setAttribute('$permissions', Permission::read(Role::users()), SetType::Append)
         ->setAttribute('$permissions', Permission::update(Role::users()), SetType::Append);

$document = $database->updateDocument(
    collection: 'movies',
    id: $document->getId(),
    document: $document
);

// Info regarding who has permission to read, update and delete a document
$document->getRead(); // returns an array of roles that have permission to read the document
$document->getUpdate(); // returns an array of roles that have permission to update the document
$document->getDelete(); // returns an array of roles that have permission to delete the document

// Get document with all attributes
$database->getDocument(
    collection: 'movies',
    id: $document->getId()
);

// Get document with a sub-set of attributes
$database->getDocument(
    collection: 'movies',
    id: $document->getId(),
    queries: [
        Query::select(['name', 'director', 'year'])
    ]
);

// Find documents

// Query Types
$queries = [
    Query::equal(attribute: 'name', values: ['Captain Marvel', 'Frozen']),
    Query::notEqual(attribute: 'director', value: 'Unknown'),
    Query::lessThan(attribute: 'year', value: 2030),
    Query::lessThanEqual(attribute: 'year', value: 2030),
    Query::greaterThan(attribute: 'year', value: 2000),
    Query::greaterThanEqual(attribute: 'year', value: 2000),
    Query::containsAny(attribute: 'genres', values: ['action', 'comics']), // array attributes
    Query::containsString(attribute: 'director', values: ['Boden']), // string attributes
    Query::between(attribute: 'year', start: 2000, end: 2030),
    Query::search(attribute: 'name', value: 'Marvel'), // needs a fulltext index on the attribute
    Query::select(['name', 'year']),
    Query::orderDesc(attribute: 'year'),
    Query::orderAsc(attribute: 'name'),
    Query::isNull(attribute: 'director'),
    Query::isNotNull(attribute: 'director'),
    Query::startsWith(attribute: 'name', value: 'Captain'),
    Query::endsWith(attribute: 'director', value: 'Fleck'),
    Query::limit(value: 35),
    Query::offset(value: 0),
];

$database->find(
    collection: 'movies',
    queries: [
        Query::equal(attribute: 'name', values: ['Captain Marvel (2019)']),
        Query::notEqual(attribute: 'year', value: 2020)
    ]
);

// Find a document
$database->findOne(
    collection: 'movies',
    queries: [
        Query::equal(attribute: 'name', values: ['Captain Marvel (2019)']),
        Query::lessThan(attribute: 'year', value: 2030)
    ]
);

// Get count of documents
$database->count(
    collection: 'movies',
    queries: [
        Query::equal(attribute: 'name', values: ['Captain Marvel (2019)']),
        Query::greaterThan(attribute: 'year', value: 2000)
    ],
    max: 1000 // Max is optional
);

// Get the sum of an attribute from all the documents
$database->sum(
    collection: 'movies',
    attribute: 'price',
    queries: [
        Query::greaterThan(attribute: 'year', value: 2000)
    ],
    max: null // max = null means no limit
);

// Delete a cached document
// Note: Cached Documents or Collections are automatically deleted when a document or collection is updated or deleted
$database->purgeCachedDocument(
    collectionId: 'movies',
    id: $document->getId()
);

// Delete a document
$database->deleteDocument(
    collection: 'movies',
    id: $document->getId()
);
```

### Joins and Aggregations

The SQL adapters run joins and aggregations (`Capability::Joins`, `Capability::Aggregations`). A joined collection is read with the same permissions as a direct read of it, and its attributes come back under the join's alias.

```php
use Utopia\Database\Document;
use Utopia\Database\Query;

$movie = $database->createDocument('movies', new Document([
    'name' => 'Frozen',
    'director' => 'Chris Buck & Jennifer Lee',
    'year' => 2013,
    'price' => 19.99,
    'active' => true,
    'genres' => ['animation'],
]));

$database->createDocument('reviews', new Document([
    'body' => 'A classic',
    'movie' => $movie->getId(),
]));

// Join the reviews to their movies, aliased 'm'
$database->find('reviews', [
    Query::join('movies', 'movie', '$id', '=', 'm'),
    Query::select(['body', 'm.name']),
]);

// Aggregate: one row per group, holding the groups and the aggregates
$database->find('movies', [
    Query::count('*', 'movies'),
    Query::avg('price', 'averagePrice'),
    Query::groupBy(['active']),
]);
```

## System Requirements

Utopia Framework requires PHP 8.5 or later. We recommend using the latest PHP version whenever possible.

## Contributing

Thank you for considering contributing to the Utopia Framework! 
Check out the [CONTRIBUTING.md](https://github.com/utopia-php/database/blob/main/CONTRIBUTING.md) file for more information.

## Copyright and License

The MIT License (MIT) [http://www.opensource.org/licenses/mit-license.php](http://www.opensource.org/licenses/mit-license.php)
