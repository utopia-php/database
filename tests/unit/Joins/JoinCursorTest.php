<?php

namespace Tests\Unit\Joins;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * A cursor over a joined read names the row the read returned: its order values, the main `$sequence` and each
 * joined `$id`. Paging walks every joined row exactly once, in both directions, through rows an outer join did not
 * match on either side; a cursor missing one of those values is refused by name.
 */
final class JoinCursorTest extends TestCase
{
    private Database $database;

    protected function setUp(): void
    {
        $this->useDatabase(new SQLite(new PDO('sqlite::memory:')));
    }

    public function testCursorWithoutItsJoinedOrderValueIsRefusedByName(): void
    {
        $queries = [Query::join('notes', '$id', 'author', '=', 'n'), Query::orderAsc('n.rank')];
        $cursor = $this->database->find('authors', [...$queries, Query::limit(1)])[0];
        $cursor->removeAttribute('n.rank');
        $this->assertNotNull($cursor->getAttribute('rank'), 'the main document\'s attribute of the same name is there to fall back to');

        $this->expectException(OrderException::class);
        $this->expectExceptionMessage("Cursor has no value for order attribute 'n.rank'");

        $this->database->find('authors', [...$queries, Query::cursorAfter($cursor)]);
    }

    private function useDatabase(SQLite $adapter): void
    {
        $this->database = new Database($adapter, new Cache(new NoCache()));
        $this->database
            ->setDatabase('join_cursor')
            ->setNamespace('join_cursor_'.\uniqid())
            ->setAuthorization(new Authorization());
        $this->database->addHook(new Permissions());
        $this->database->create();

        $this->database->createCollection(new Collection(
            id: 'authors',
            attributes: [Attribute::string(key: 'name', size: 16), Attribute::integer(key: 'rank', required: false)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $this->database->createCollection(new Collection(
            id: 'notes',
            attributes: [
                Attribute::string(key: 'author', size: 16),
                Attribute::integer(key: 'rank', required: false),
                Attribute::string(key: 'label', size: 16),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        foreach (['a1' => 1, 'a2' => 2, 'a3' => 3] as $id => $rank) {
            $this->createDocument('authors', $id, ['name' => $id, 'rank' => $rank]);
        }

        foreach ([
            'n1' => ['a1', 1, 'x'],
            'n2' => ['a1', 1, 'x'],
            'n3' => ['a1', 2, 'y'],
            'n4' => ['a2', 1, 'y'],
            'n5' => ['zz', 9, 'z'],
            'n6' => ['a2', null, 'x'],
        ] as $id => [$author, $rank, $label]) {
            $this->createDocument('notes', $id, ['author' => $author, 'rank' => $rank, 'label' => $label]);
        }
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function createDocument(string $collection, string $id, array $attributes): void
    {
        $this->database->createDocument($collection, new Document([
            '$id' => $id,
            '$permissions' => [Permission::read(Role::any())],
            ...$attributes,
        ]));
    }
}
