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

Attribute filters encode an attribute's value before it is saved and decode it after it is read. List a filter's name in an attribute's `filters`.

- `Database::addFilter($name, $encode, $decode)` registers a filter for every `Database` instance in the process. Its callbacks receive the value, the document and the database.
- A `Utopia\Database\Filter\Codec` (`name()`, `encode()`, `decode()`) belongs to the instances you give it to: pass a list of codecs to the `Database` constructor, or put them on a `Filter\Registry` given to several instances with `setFilters()`. `Filter\Callback` builds a codec from two closures. Codecs override global filters of the same name.

```php
use Utopia\Database\Database;
use Utopia\Database\Filter\Callback;

$database = new Database($adapter, $cache, [
    new Callback('trim', fn (mixed $value) => \trim($value), fn (mixed $value) => $value),
]);
```

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

Attributes are built with a factory per type on `Utopia\Database\Attribute` (`Attribute::string()`, `integer()`, `lineString()`, ...). Their types are cases of `Utopia\Query\Schema\ColumnType`: `String`, `Varchar`, `Text`, `MediumText`, `LongText`, `Integer`, `BigInteger`, `Float`, `Double`, `Boolean`, `Datetime`, `Id`, `Relationship`, `Object`, `Point`, `Linestring`, `Polygon` and `Vector`. `Attribute::TYPES` lists them, and object, spatial and vector attributes need an adapter that supports them. Attributes of the other types can hold an array of values (`array: true`). Arrays and objects are encoded to JSON when stored and decoded back when fetched, where the adapter has no native type for them.

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

`Utopia\Database\PDO` wraps PHP's PDO: it reconnects when a connection is lost outside a transaction and retries the call. Each SQL adapter also accepts a plain `PDO`. Pass the PDO attributes your application needs; the ones below are what the adapters expect.

#### MariaDB

```php
require_once __DIR__ . '/vendor/autoload.php';

use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Database;
use Utopia\Database\PDO;

$host = 'mariadb';
$port = 3306;

$pdo = new PDO("mysql:host={$host};port={$port};charset=utf8mb4", 'root', 'password', [
    \PDO::ATTR_TIMEOUT => 3,
    \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
    \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
    \PDO::ATTR_EMULATE_PREPARES => true,
    \PDO::ATTR_STRINGIFY_FETCHES => true,
]);

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

$host = 'mysql';
$port = 3306;

$pdo = new PDO("mysql:host={$host};port={$port};charset=utf8mb4", 'root', 'password', [
    \PDO::ATTR_TIMEOUT => 3,
    \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
    \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
    \PDO::ATTR_EMULATE_PREPARES => true,
    \PDO::ATTR_STRINGIFY_FETCHES => true,
]);

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

$host = 'postgres';
$port = 5432;

$pdo = new PDO("pgsql:host={$host};port={$port}", 'root', 'password', [
    \PDO::ATTR_TIMEOUT => 3,
    \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
    \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
    \PDO::ATTR_EMULATE_PREPARES => true,
    \PDO::ATTR_STRINGIFY_FETCHES => true,
]);

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

$path = '/path/to/database.sqlite';

$pdo = new PDO("sqlite:{$path}", null, null, [
    \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
    \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
    \PDO::ATTR_STRINGIFY_FETCHES => true,
]);

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

$client = new Client('database', 'mongo', 27017, 'root', 'password', true);

$cache = new Cache(new Memory()); // or use any cache adapter you wish

$database = new Database(new Mongo($client), $cache);
```

### Hooks

Document permissions and relationships are hooks. Register both after you create the `Database`:

```php
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;

// Writes, moves and deletes the permission rows that MariaDB, MySQL and SQLite check document permissions
// against. PostgreSQL checks the row's own _permissions column; MongoDB, Memory and Redis keep permissions
// with the document.
$database->addHook(new Permissions());

// Populates related documents and handles nested writes and cascades
$database->addHook(new Relationships());
```

Without `Hook\Permissions`, MariaDB, MySQL and SQLite neither write nor remove permission rows, and without
`Hook\Relationships` no `onDelete` rule runs. See
[UPGRADE.md](UPGRADE.md#register-the-permission-and-relationship-hooks).

To act on database events, register a lifecycle hook. Its `handle()` receives one typed event object per event, such as `Event\Document\Created` with its `collection` and `document`. A hook that also implements `Selective` receives only the events its `handles()` accepts, and one that implements `Named` replaces the hook already registered under its name and can be silenced by name with `silent()`.

```php
use Utopia\Database\Event;
use Utopia\Database\Event\Document\Created;
use Utopia\Database\Event\Document\Deleted;
use Utopia\Database\Event\Domain;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Hook\Named;
use Utopia\Database\Hook\Selective;

final class AuditLog implements Lifecycle, Named, Selective
{
    /** @var list<string> */
    public array $entries = [];

    public function getName(): string
    {
        return 'audit-log';
    }

    public function handles(Event $event): bool
    {
        return $event === Event::DocumentCreate || $event === Event::DocumentDelete;
    }

    public function handle(Domain $event): void
    {
        if ($event instanceof Created || $event instanceof Deleted) {
            $this->entries[] = $event->event->value.':'.$event->document->getId();
        }
    }
}

$auditLog = new AuditLog();
$database->addHook($auditLog);

// Run a callback without the named hooks, or without any hook when no names are given
$database->silent(fn () => $database->ping(), ['audit-log']);
```

A `Utopia\Database\Hook\Transform` rewrites SQL statements before they run, and a `Utopia\Database\Hook\Decorator` modifies the documents that reads and writes return. Both are registered with `addHook()` too, and `removeHook()` unregisters a hook. `Event\DispatcherHook` forwards events to listeners registered per event class and to a PSR-14 dispatcher.

### Database Methods

```php
use Utopia\Database\Capability;

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

// Renames a database
$database->update(
    database: 'dbName',
    new: 'archive'
);

// Returns an array of all databases
$database->list();

// Check if collection exists
$database->collectionExists(
    collection: 'users',
    database: 'dbName'
);

// Ping database it returns true if the database is alive
$database->ping();

// Get Database Adapter
$database->getAdapter();

// Limits, capabilities and features of the adapter
$profile = $database->profile();
$profile->limits->keywords; // names that cannot be used as attribute keys
$profile->supports(Capability::Joins);
```

### Collection Methods

```php
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\CollectionUpdate;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Query\OrderDirection;

// Creates a new collection named 'users'. The SQL adapters store it as the table '$namespace_users',
// with the table '$namespace_users_perms' for its document permissions.
$database->createCollection(Collection::create(
    id: 'users',
    attributes: [
        Attribute::string('name', size: 256),
        Attribute::integer('age'),
    ],
    indexes: [
        Index::key('idx_name', ['name'], lengths: [256], orders: [OrderDirection::Asc]),
        Index::key('idx_name_age', ['name', 'age'], lengths: [128, null], orders: [OrderDirection::Asc, OrderDirection::Desc]),
    ],
    permissions: [
        Permission::create(Role::any()),
        Permission::read(Role::any()),
    ],
    documentSecurity: true,
));

// Update Collection Permissions; a field left null keeps its stored value
$database->updateCollection('users', new CollectionUpdate(
    permissions: [
        Permission::create(Role::any()),
        Permission::read(Role::any()),
        Permission::update(Role::any()),
        Permission::delete(Role::any()),
    ],
    documentSecurity: true,
));

// Get Collection; throws Exception\NotFound when it does not exist
$collection = $database->getCollection('users');
$collection->attributes(); // list of Attribute
$collection->indexes(); // list of Index

// Find Collection; null when it does not exist
$database->findCollection('users');

// List Collections
$database->listCollections(
    limit: 25,
    offset: 0
);

// Delete cached documents of a collection
$database->purgeCachedCollection('users');

// Deletes the collection and its permissions table
$database->createCollection(Collection::create(id: 'drafts'));
$database->deleteCollection('drafts');
```

### Attribute Methods

```php
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Collection;
use Utopia\Database\Format;
use Utopia\Database\IntegerWidth;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;
use Utopia\Validator\Range;

$database->createCollection(Collection::create(
    id: 'movies',
    permissions: [
        Permission::create(Role::any()),
        Permission::read(Role::any()),
        Permission::update(Role::any()),
        Permission::delete(Role::any()),
    ],
));

// Creates a new attribute named 'name' in the 'movies' collection and returns the stored attribute.
// Every type has a factory: Attribute::string(), integer(), float(), boolean(), datetime(), point(), ...
$database->createAttribute('movies', Attribute::string('name', size: 128, required: true));

// New attribute with optional parameters
$database->createAttribute('movies', Attribute::string(
    'genres',
    size: 128,
    required: false,
    default: null,
    array: true,
    filters: [],
));

// Creates several attributes at once
$database->createAttributes('movies', [
    Attribute::string('director', size: 128),
    Attribute::integer('year'),
    Attribute::integer('views', signed: false, width: IntegerWidth::Bits64),
    Attribute::float('price'),
    Attribute::boolean('active'),
]);

// A format is a validator registered for an attribute type
Structure::addFormat(
    'year',
    fn (array $attribute) => new Range($attribute['formatOptions']['min'] ?? 0, $attribute['formatOptions']['max'] ?? 9999),
    ColumnType::Integer
);

// Updates an attribute. Only the fields given change; default: null and format: null remove them.
$database->updateAttribute('movies', 'year', new AttributeUpdate(
    required: true,
    format: new Format('year', ['min' => 1888, 'max' => 2100]),
));

$database->updateAttribute('movies', 'director', new AttributeUpdate(default: 'Unknown'));

// Check if attribute can be added to a collection
$database->checkAttribute('movies', Attribute::integer('rating'));

// Get Adapter attribute limit
$database->getLimitForAttributes(); // if 0 then no limit

// Get Adapter index limit
$database->getLimitForIndexes();

// Renames the attribute from old to new in the 'movies' collection.
$database->createAttribute('movies', Attribute::string('tagline', size: 256));
$database->renameAttribute(
    collection: 'movies',
    old: 'tagline',
    new: 'slogan'
);

// Deletes the attribute in the 'movies' collection.
$database->deleteAttribute('movies', 'slogan');
```

### Index Methods

```php
use Utopia\Database\Index;
use Utopia\Query\OrderDirection;

// Every index type has a factory: Index::key(), unique(), fulltext(), trigram(), spatial(), object(), ttl(),
// hnswEuclidean(), hnswCosine() and hnswDot(). Orders are OrderDirection::Asc and OrderDirection::Desc.

// Creates a new index named 'index1' in the 'movies' collection and returns the stored index.
$database->createIndex('movies', Index::key(
    'index1',
    ['name', 'year'],
    lengths: [128, null],
    orders: [OrderDirection::Asc, OrderDirection::Desc]
));

// Creates several indexes and writes the collection definition once
$database->createIndexes('movies', [
    Index::fulltext('index_name_search', ['name']),
    Index::unique('index_director_year', ['director', 'year']),
]);

// Rename index from old to new in the 'movies' collection.
$database->renameIndex(
    collection: 'movies',
    old: 'index1',
    new: 'index2'
);

// Deletes the index in the 'movies' collection.
$database->deleteIndex('movies', 'index2');
```

### Relationship Methods

```php
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Permission;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\Role;

// Each relationship type has a factory: Relationship::oneToOne(), oneToMany(), manyToOne() and manyToMany().
// What happens to related documents when a document is deleted is a RelationshipDeleteAction:
// Restrict (the default), Cascade or SetNull.

// Creates a relationship between the two collections with the default reference attributes:
// 'users' on 'movies', and 'movies' on 'users'
$database->createRelationship('movies', Relationship::oneToOne('users', twoWay: true));

// Create a relationship with custom reference attributes
$database->createCollection(Collection::create(
    id: 'reviews',
    attributes: [Attribute::string('body', size: 1024)],
    permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
));

$database->createRelationship('movies', Relationship::oneToMany(
    'reviews',
    key: 'reviews',
    twoWay: true,
    twoWayKey: 'movie',
    onDelete: RelationshipDeleteAction::Cascade
));

// Update the relationship with the default reference attributes
$database->updateRelationship('movies', 'users', new RelationshipUpdate(
    onDelete: RelationshipDeleteAction::SetNull
));

// Update the relationship with custom reference attributes
$database->updateRelationship('movies', 'users', new RelationshipUpdate(
    key: 'viewer',
    twoWayKey: 'favoriteMovie',
    twoWay: true
));

// Delete the relationship with the default or custom reference attributes
$database->deleteRelationship('movies', 'viewer');
```

### Document Methods

```php
use Utopia\Database\Document;
use Utopia\Database\Id;
use Utopia\Database\Permission;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\SetType;

// Id helpers
Id::unique(padding: 12); // Creates an id of 13 characters from uniqid() followed by 12 random hex characters
Id::custom(id: 'my_user_3235');

// Role helpers
Role::any();
Role::guests();
Role::user(
    identifier: Id::unique(),
    status: 'verified' // optional
);
Role::users();
Role::team(
    identifier: Id::unique()
);
Role::team(
    identifier: Id::unique(),
    dimension: '123' // team:id/dimension
);
Role::label(
    identifier: 'admin'
);
Role::member(
    identifier: Id::unique()
);

// Permission helpers
Permission::read(Role::any());
Permission::create(Role::user(Id::unique()));
Permission::update(Role::user(Id::unique(padding: 23)));
Permission::delete(Role::user(Id::custom(id: 'my_user_3235')));

// To create a document
$document = new Document([
    '$permissions' => [
        Permission::read(Role::any()),
        Permission::update(Role::user(Id::custom('1x'))),
        Permission::delete(Role::user(Id::unique(12))),
    ],
    '$id' => Id::unique(),
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

// Roles that have permission to read, update and delete the document
$document->getPermissionsByType(PermissionType::Read);
$document->getPermissionsByType(PermissionType::Update);
$document->getPermissionsByType(PermissionType::Delete);

// The document as an array, with or without some top-level keys
$document->only(['name', 'year']);
$document->except(['$permissions']);

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

// Read every match in batches of 100; a limit() caps how many are yielded
foreach ($database->cursor('movies', [Query::greaterThan('year', 2000)], batchSize: 100) as $movie) {
    $movie->getId();
}

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
$database->purgeCachedDocument('movies', $document->getId());

// Delete documents in batches of at most Database::BATCH_SIZE; onNext receives each deleted document
$deleted = [];
$database->deleteDocuments(
    collection: 'movies',
    queries: [Query::lessThan(attribute: 'year', value: 1900)],
    onNext: function (Document $document) use (&$deleted): void {
        $deleted[] = $document->getId();
    }
);

// Delete a document
$database->deleteDocument(
    collection: 'movies',
    id: $document->getId()
);
```

### Joins and Aggregations

The SQL adapters run joins and aggregations (`Capability::Joins`, `Capability::Aggregations`). A join names its alias and lists its conditions; a joined collection is read with the same permissions as a direct read of it, and its attributes come back under the join's alias. Aggregations run through `aggregate()`, which returns one row per group.

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
    Query::join('movies', 'm', [Query::on('movie', '$id')]),
    Query::select(['body', 'm.name']),
]);

// Aggregate: one row per group, holding the groups and the aggregates
$database->aggregate('movies', [
    Query::count('*', 'movies'),
    Query::avg('price', 'averagePrice'),
    Query::groupBy(['active']),
]);
```

Index the attributes your join conditions compare. On MariaDB and MySQL, a one-to-many join can sort the whole join before the limit; see [Joins](UPGRADE.md#joins) in the upgrade guide for when it does not.

## System Requirements

Utopia Framework requires PHP 8.5 or later. We recommend using the latest PHP version whenever possible.

## Contributing

Thank you for considering contributing to the Utopia Framework! 
Check out the [CONTRIBUTING.md](https://github.com/utopia-php/database/blob/main/CONTRIBUTING.md) file for more information.

## Copyright and License

The MIT License (MIT) [http://www.opensource.org/licenses/mit-license.php](http://www.opensource.org/licenses/mit-license.php)
