<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\PDO as DatabasePDO;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * SQLite has no REGEXP of its own; the adapter registers a PCRE user function under that name, and
 * Query::regex compiles to it.
 */
final class SQLiteRegexTest extends TestCase
{
    private const string COLLECTION = 'words';

    private const array NAMES = ['abc', 'axc', 'abcd', 'ABC', 'a.c', 'xabc'];

    private const string PATTERN = '^a.c$';

    public function testRegexMatchesThroughTheUserFunction(): void
    {
        $database = $this->database();

        $names = \array_map(
            static fn (Document $document): mixed => $document->getAttribute('name'),
            $database->find(self::COLLECTION, [Query::regex('name', self::PATTERN)]),
        );

        $this->assertSame(['abc', 'axc', 'a.c'], $names);
        $this->assertSame(3, $database->count(self::COLLECTION, [Query::regex('name', self::PATTERN)]));
    }

    public function testRegexIsAdvertisedOnlyWithTheUserFunction(): void
    {
        $registered = new SQLite(new DatabasePDO('sqlite::memory:', null, null));

        $this->assertTrue($registered->supports(Capability::Regex));
        $this->assertTrue($registered->supports(Capability::PCRE));

        $unregistered = new SQLite(new class () extends PDO {
            public function __construct()
            {
            }

            public function createFunction(string $name, callable $callback, int $arguments = -1, int $flags = 0): bool
            {
                return false;
            }
        });

        $this->assertFalse($unregistered->supports(Capability::Regex));
        $this->assertFalse($unregistered->supports(Capability::PCRE));
    }

    private function database(): Database
    {
        $database = new Database(new SQLite(new DatabasePDO('sqlite::memory:', null, null)), new Cache(new None()));
        $database
            ->setDatabase('regex')
            ->setNamespace('regex')
            ->setAuthorization(new Authorization());
        $database->create();

        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string('name', size: 32)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
        ));

        foreach (self::NAMES as $name) {
            $database->createDocument(self::COLLECTION, new Document(['name' => $name]));
        }

        return $database;
    }
}
