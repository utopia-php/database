<?php

// Runs in its own process: never load cache 5 and cache 6 classes together.
require getenv('DATABASE_TEST_LEGACY_AUTOLOAD') ?: (is_file('/opt/database-legacy/vendor/autoload.php') ? '/opt/database-legacy/vendor/autoload.php' : __DIR__.'/vendor/autoload.php');

use Utopia\Cache\Adapter\Redis as RedisCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\Query as QueryCache;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Permission;
use Utopia\Database\Role;

$arguments = json_decode($argv[1], true, flags: JSON_THROW_ON_ERROR);
$redis = new Redis();
$redis->connect(getenv('DATABASE_TEST_CACHE_HOST') ?: 'redis', (int) (getenv('DATABASE_TEST_CACHE_PORT') ?: 6379));
$cache = new Cache(new RedisCache($redis));
$database = new Database(new SQLite(new PDO('sqlite:'.$arguments['path'])), $cache);
$database->setDatabase('bridge')->setNamespace($arguments['namespace']);
$database->setQueryCache(new QueryCache($cache));

$result = match ($arguments['action']) {
    'init' => (function () use ($database): bool {
        $database->create();
        $database->createCollection(Collection::create(id: 'users', attributes: [Attribute::string(key: 'name')], permissions: [Permission::read(Role::any()), Permission::create(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())]));
        $database->createDocument('users', new Document(['$id' => 'user', 'name' => 'original']));
        return true;
    })(),
    'get' => $database->getDocument('users', $arguments['id'] ?? 'user')->getAttribute('name'),
    'find' => array_map(fn (Document $document) => $document->getAttribute('name'), $database->find('users')),
    'update' => $database->updateDocument('users', 'user', new Document(['name' => $arguments['value']]))->getAttribute('name'),
    'create' => $database->createDocument('users', new Document(['$id' => $arguments['id'], 'name' => $arguments['value']]))->getAttribute('name'),
    'delete' => $database->deleteDocument('users', $arguments['id']),
    'attributes' => array_map(fn (Document $attribute) => $attribute->getId(), $database->getCollection('users')->getAttribute('attributes')),
    'hold' => $database->withTransaction(function () use ($database, $arguments): string {
        $database->updateDocument('users', 'user', new Document(['name' => $arguments['value']]));
        echo "ready\n";
        fflush(STDOUT);
        if (fgets(STDIN) !== "commit\n") {
            throw new RuntimeException('Commit signal missing');
        }
        return $arguments['value'];
    }),
};
echo json_encode($result, JSON_THROW_ON_ERROR)."\n";
