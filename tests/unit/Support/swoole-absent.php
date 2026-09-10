<?php

/**
 * Exercises the lifecycle-hook paths that reach Database::getEventContext(), plus
 * Connection::hasError(), in a process where ext-swoole is unavailable.
 *
 * Run by SwooleAbsentTest through a subprocess started with -n, because an
 * extension cannot be unloaded from inside a running interpreter.
 */

require dirname(__DIR__, 3) . '/vendor/autoload.php';

use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Collection;
use Utopia\Database\Connection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

echo 'swoole=' . (extension_loaded('swoole') ? '1' : '0') . PHP_EOL;

$database = new Database(new Memory(), new Cache(new None()));
$database->setDatabase('utopiaTests')->setNamespace('swoole_absent');
$database->create();
$database->getAuthorization()->addRole(Role::any()->toString());

echo 'create=ok' . PHP_EOL;

echo 'silent=' . $database->silent(fn () => 'ok') . PHP_EOL;

$database->createCollection(new Collection(
    id: 'logs',
    permissions: [
        Permission::read(Role::any()),
        Permission::create(Role::any()),
        Permission::delete(Role::any()),
    ],
    documentSecurity: false,
));

foreach (['a', 'b', 'c'] as $id) {
    $database->createDocument('logs', new Document(['$id' => $id]));
}

$deleted = $database->deleteDocuments('logs', [Query::limit(10)]);

echo 'deleted=' . $deleted . PHP_EOL;
echo 'remaining=' . \count($database->find('logs', [Query::limit(10)])) . PHP_EOL;
echo 'hasError=' . (Connection::hasError(new RuntimeException('boom')) ? '1' : '0') . PHP_EOL;
