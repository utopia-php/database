<?php

namespace Tests\Unit\Joins;

use Closure;
use PDO;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * `alias.*` selects the joined row as a direct read of the joined collection returns it, alone or next to other
 * selects, and an order may name a joined attribute by its bare name when exactly one join declares it.
 */
final class JoinedShapesTest extends TestCase
{
    private const array JOINED_KEYS = ['it.$createdAt', 'it.$id', 'it.$permissions', 'it.$sequence', 'it.$updatedAt', 'it.code', 'it.name', 'it.price'];

    public function testAJoinWildcardSelectsTheJoinedColumnsNextToOtherSelects(): void
    {
        $database = $this->database();
        $item = Query::join('items', 'item', 'code', '=', 'it');

        $rows = $database->find('orders', [$item, Query::select(['name', 'it.*']), Query::orderAsc('$id')]);
        $this->assertSame(['x', 'y', 'x'], \array_map(static fn (Document $row): mixed => $row->getAttribute('name'), $rows));
        $this->assertSame(['apple', 'banana', 'apple'], \array_map(static fn (Document $row): mixed => $row->getAttribute('it.name'), $rows));
        $this->assertSame(self::JOINED_KEYS, $this->joinedKeys($rows[0]));
        $this->assertNull($rows[0]->getAttribute('quantity'), 'an attribute of the main collection the select leaves out');

        $alone = $database->find('orders', [$item, Query::select(['it.*']), Query::orderAsc('$id')]);
        $this->assertSame(self::JOINED_KEYS, $this->joinedKeys($alone[0]));
        $this->assertNull($alone[0]->getAttribute('name'));
        $this->assertSame('o1', $alone[0]->getId());

        $withWildcard = $database->find('orders', [$item, Query::select(['*', 'it.*']), Query::orderAsc('$id')]);
        $this->assertSame(
            $this->mainAttributes($database->find('orders', [$item, Query::orderAsc('$id')])),
            $this->mainAttributes($withWildcard),
            'next to * the main document is returned as * alone returns it',
        );
        $this->assertSame(self::JOINED_KEYS, $this->joinedKeys($withWildcard[0]), 'next to * it adds the whole joined row');
        $this->assertSame(
            $this->arrays($database->find('orders', [$item, Query::select(['name', 'it.*']), Query::orderAsc('$id')])),
            $this->arrays($database->find('orders', [$item, Query::select(['name', 'it.*', 'it.name']), Query::orderAsc('$id')])),
            'a joined attribute named as well is selected once',
        );

        $both = $database->find('orders', [$item, Query::join('extras', 'item', 'code', '=', 'ex'), Query::select(['quantity', 'ex.*'])]);
        $this->assertCount(2, $both);
        $this->assertSame(['ex.$createdAt', 'ex.$id', 'ex.$permissions', 'ex.$sequence', 'ex.$updatedAt', 'ex.code', 'ex.price'], $this->joinedKeys($both[0]));

        $document = $database->getDocument('orders', 'o2', [$item, Query::select(['name', 'it.*'])]);
        $this->assertSame('y', $document->getAttribute('name'));
        $this->assertSame('banana', $document->getAttribute('it.name'));
        $this->assertSame(20, $document->getAttribute('it.price'));
    }

    public function testAJoinWildcardIsRefusedWhereItNamesNoJoinOrInAnAggregation(): void
    {
        $database = $this->database();
        $item = Query::join('items', 'item', 'code', '=', 'it');

        $this->assertRefused(
            'Invalid query: Cannot select "it.*": an aggregation query can only select the attributes it groups by',
            fn (): mixed => $database->find('orders', [$item, Query::count('*', 'orders'), Query::groupBy(['it.name']), Query::select(['it.*'])]),
        );
        $this->assertRefused(
            'Invalid query: Attribute not found in schema: zz',
            fn (): mixed => $database->find('orders', [$item, Query::select(['name', 'zz.*'])]),
        );
        $this->assertRefused(
            'Invalid query: Attribute not found in schema: it',
            fn (): mixed => $database->find('orders', [Query::select(['name', 'it.*'])]),
            'without the join',
        );
    }

    public function testAnOrderOnABareJoinedNameReadsTheJoin(): void
    {
        foreach (['join' => [false, 'join'], 'emulated full outer join' => [false, 'fullOuterJoin'], 'native full outer join' => [true, 'fullOuterJoin']] as $case => [$native, $method]) {
            $database = $this->database($native);
            $item = Query::$method('items', 'item', 'code', '=', 'it');

            $this->assertSame(
                $this->ids($database->find('orders', [$item, Query::orderDesc('it.price'), Query::orderAsc('$id')])),
                $this->ids($database->find('orders', [$item, Query::orderDesc('price'), Query::orderAsc('$id')])),
                $case,
            );
            $this->assertSame(['o2', 'o1', 'o3'], $this->ids($database->find('orders', [$item, Query::orderDesc('price'), Query::orderAsc('$id')])), $case);
            $this->assertSame(
                [['orders' => 1, 'code' => 'b'], ['orders' => 2, 'code' => 'a']],
                $this->arrays($database->find('orders', [$item, Query::count('*', 'orders'), Query::groupBy(['code']), Query::orderDesc('code')])),
                $case.': a group only the join declares, named bare',
            );
            $this->assertSame(
                [['orders' => 1, 'code' => 'b'], ['orders' => 2, 'code' => 'a']],
                $this->arrays($database->find('orders', [$item, Query::count('*', 'orders'), Query::groupBy(['it.code']), Query::orderDesc('code')])),
                $case.': a group named by its alias',
            );
        }
    }

    public function testAnOrderOnANameTheMainCollectionDeclaresReadsTheMainTable(): void
    {
        $database = $this->database();
        $item = Query::join('items', 'item', 'code', '=', 'it');

        $this->assertSame(['o2', 'o1', 'o3'], $this->ids($database->find('orders', [$item, Query::orderDesc('name'), Query::orderAsc('$id')])), 'orders are named y, x, x; their items apple, banana');
    }

    public function testAnOrderOnABareNameSeveralOrNoCollectionsDeclareIsRefused(): void
    {
        $database = $this->database();
        $joins = [Query::join('items', 'item', 'code', '=', 'it'), Query::join('extras', 'item', 'code', '=', 'ex')];
        $ambiguous = 'Attribute "price" is ambiguous across joins; qualify it with a join alias';

        $this->assertRefused('Invalid query: '.$ambiguous, fn (): mixed => $database->find('orders', [...$joins, Query::orderAsc('price')]));
        $this->assertRefused('Invalid query: Attribute not found in schema: weight', fn (): mixed => $database->find('orders', [...$joins, Query::orderAsc('weight')]));
        $this->assertSame(['o1', 'o3'], $this->ids($database->find('orders', [...$joins, Query::orderAsc('ex.price'), Query::orderAsc('$id')])));

        $database->disableValidation();
        $this->assertRefused($ambiguous, fn (): mixed => $database->find('orders', [...$joins, Query::orderAsc('price')]), 'the adapter without validation');
    }

    private function assertRefused(string $message, Closure $read, string $label = ''): void
    {
        try {
            $read();
        } catch (QueryException $error) {
            $this->assertSame($message, $error->getMessage(), $label);

            return;
        }

        $this->fail(($label === '' ? '' : $label.': ').'the shape was accepted');
    }

    /**
     * @return list<string>
     */
    private function joinedKeys(Document $row): array
    {
        $keys = \array_values(\array_filter(\array_keys($row->getArrayCopy()), static fn (string $key): bool => \str_contains($key, '.')));
        \sort($keys);

        return $keys;
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private function ids(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): string => $document->getId(), $documents));
    }

    /**
     * @param  array<Document>  $documents
     * @return list<array<string, mixed>>
     */
    private function arrays(array $documents): array
    {
        return \array_values(\array_map(static function (Document $document): array {
            $row = $document->getArrayCopy();
            unset($row['$createdAt'], $row['$updatedAt']);

            return $row;
        }, $documents));
    }

    /**
     * @param  array<Document>  $documents
     * @return list<array<string, mixed>>
     */
    private function mainAttributes(array $documents): array
    {
        return \array_map(
            static fn (array $row): array => \array_filter($row, static fn (string $key): bool => ! \str_contains($key, '.'), ARRAY_FILTER_USE_KEY),
            $this->arrays($documents),
        );
    }

    /**
     * Orders of items: o1 and o3 order a (price 10), o2 orders b (price 20); extras list a at 100.
     */
    private function database(bool $nativeFullOuterJoin = false): Database
    {
        $pdo = new PDO('sqlite::memory:');
        $database = new Database($nativeFullOuterJoin ? new NativeFullOuterJoinSQLite($pdo) : new SQLite($pdo), new Cache(new NoCache()));
        $database
            ->setDatabase('joined_shapes')
            ->setNamespace('joined_shapes_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->addHook(new Permissions());
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(
            id: 'orders',
            attributes: [
                Attribute::string(key: 'item', size: 16),
                Attribute::integer(key: 'quantity'),
                Attribute::string(key: 'name', size: 16),
            ],
            permissions: $permissions,
        ));
        $database->createCollection(Collection::create(
            id: 'items',
            attributes: [
                Attribute::string(key: 'code', size: 16),
                Attribute::integer(key: 'price'),
                Attribute::string(key: 'name', size: 16),
            ],
            permissions: $permissions,
        ));
        $database->createCollection(Collection::create(
            id: 'extras',
            attributes: [
                Attribute::string(key: 'code', size: 16),
                Attribute::integer(key: 'price'),
            ],
            permissions: $permissions,
        ));

        foreach ([['o1', 'a', 1, 'x'], ['o2', 'b', 2, 'y'], ['o3', 'a', 3, 'x']] as [$id, $item, $quantity, $name]) {
            $database->createDocument('orders', new Document(['$id' => $id, '$permissions' => [], 'item' => $item, 'quantity' => $quantity, 'name' => $name]));
        }
        foreach ([['a', 10, 'apple'], ['b', 20, 'banana']] as [$code, $price, $name]) {
            $database->createDocument('items', new Document(['$id' => $code, '$permissions' => [], 'code' => $code, 'price' => $price, 'name' => $name]));
        }
        $database->createDocument('extras', new Document(['$id' => 'a', '$permissions' => [], 'code' => 'a', 'price' => 100]));

        return $database;
    }
}
