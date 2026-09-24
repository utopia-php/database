<?php

namespace Tests\Unit\Joins;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

/**
 * A distinct read projects exactly what its select names. When the select names only joined
 * columns, the caller's select must not reach the statement next to the projection: the engine
 * would compile the raw attribute names, duplicate the columns and put a joined value on the
 * main document's bare key.
 */
final class DistinctJoinedSelectTest extends TestCase
{
    private Database $database;

    protected function setUp(): void
    {
        $this->useDatabase(new SQLite(new PDO('sqlite::memory:')));
    }

    /**
     * @return iterable<string, array{Method, bool, list<mixed>}>
     */
    public static function joins(): iterable
    {
        yield 'inner join' => [Method::Join, false, ['side-one']];
        yield 'left join' => [Method::LeftJoin, false, [null, 'side-one']];
        yield 'emulated full outer join' => [Method::FullOuterJoin, false, [null, 'side-one', 'side-two']];
        yield 'native full outer join' => [Method::FullOuterJoin, true, [null, 'side-one', 'side-two']];
    }

    /**
     * @param  list<mixed>  $labels
     */
    #[DataProvider('joins')]
    public function testDistinctSelectOfOnlyJoinedColumnsKeepsThemUnderTheirAlias(Method $join, bool $native, array $labels): void
    {
        if ($native) {
            $this->useDatabase(new NativeFullOuterJoinSQLite(new PDO('sqlite::memory:')));
        }

        $rows = $this->database->find('main', [
            new Query($join, 'side', ['code', '=', 'code', 's']),
            Query::distinct(),
            Query::select(['s.label']),
            Query::orderAsc('s.label'),
        ]);

        $this->assertSame($labels, \array_map(static fn (Document $row): mixed => $row->getAttribute('s.label'), $rows));
        foreach ($rows as $row) {
            $this->assertArrayNotHasKey('label', $row->getArrayCopy(), 'a joined value never lands on the main document\'s bare key');
        }
    }

    public function testDistinctSelectOfAJoinedInternalAttributeIsProjectedUnderItsAlias(): void
    {
        $rows = $this->database->find('main', [
            Query::join('side', 'code', 'code', '=', 's'),
            Query::distinct(),
            Query::select(['s.$id']),
        ]);

        $this->assertSame(['s1'], \array_map(static fn (Document $row): mixed => $row->getAttribute('s.$id'), $rows));
    }

    private function useDatabase(SQLite $adapter): void
    {
        $this->database = new Database($adapter, new Cache(new NoCache()));
        $this->database
            ->setDatabase('distinct_joined_select')
            ->setNamespace('distinct_joined_select_'.\uniqid())
            ->setAuthorization(new Authorization());
        $this->database->addHook(new Permissions());
        $this->database->create();

        foreach (['main', 'side'] as $collection) {
            $this->database->createCollection(new Collection(
                id: $collection,
                attributes: [Attribute::string(key: 'code', size: 16), Attribute::string(key: 'label', size: 32)],
                permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            ));
        }

        $this->createDocument('main', 'm1', ['code' => 'one', 'label' => 'main-one']);
        $this->createDocument('main', 'm2', ['code' => 'two', 'label' => 'main-two']);
        $this->createDocument('side', 's1', ['code' => 'one', 'label' => 'side-one']);
        $this->createDocument('side', 's2', ['code' => 'three', 'label' => 'side-two']);
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
