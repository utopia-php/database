<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
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

/**
 * A join without a select returns the main document plus, under each join alias, the joined
 * collection's own attributes and its `$id` — never the joined table's internal columns, and never a
 * joined value under a bare name, where it would pass for an attribute of the main document.
 */
final class JoinImplicitProjectionTest extends TestCase
{
    private const array MAIN_KEYS = ['$collection', '$createdAt', '$id', '$permissions', '$sequence', '$updatedAt', 'name'];

    public function testJoinWithoutSelectReturnsJoinedAttributesUnderTheAlias(): void
    {
        $database = $this->database(sharedTables: false);

        $rows = $database->find('customers', [
            Query::join('orders', '$id', 'customerId', '=', 'ord'),
        ]);

        $this->assertCount(1, $rows);
        $this->assertSame($this->keys('ord'), $this->sortedKeys($rows[0]));
        $this->assertSame('c1', $rows[0]->getId());
        $this->assertSame('Alice', $rows[0]->getAttribute('name'));
        $this->assertSame('o1', $rows[0]->getAttribute('ord.$id'));
        $this->assertSame('c1', $rows[0]->getAttribute('ord.customerId'));
        $this->assertSame(10, $rows[0]->getAttribute('ord.total'));
        $this->assertSame('first-secret', $rows[0]->getAttribute('ord.secret'));
    }

    public function testGeneratedAliasCarriesTheJoinedAttributes(): void
    {
        $database = $this->database(sharedTables: false);

        $rows = $database->find('customers', [
            Query::leftJoin('orders', '$id', 'customerId'),
        ]);

        $this->assertCount(2, $rows);
        foreach ($rows as $row) {
            $this->assertSame($this->keys('j0'), $this->sortedKeys($row));
        }
        $totals = \array_map(static fn (Document $row): mixed => $row->getAttribute('j0.total'), $rows);
        \sort($totals);
        $this->assertSame([null, 10], $totals);
    }

    public function testEachAliasOfOneCollectionCarriesItsOwnRow(): void
    {
        $database = $this->database(sharedTables: false);

        $rows = $database->find('customers', [
            Query::join('orders', '$id', 'customerId', '=', 'first'),
            Query::leftJoin('orders', '$id', 'customerId', '=', 'second'),
        ]);

        $this->assertCount(1, $rows);
        $this->assertSame(
            $this->keys('first', 'second.$id', 'second.customerId', 'second.secret', 'second.total'),
            $this->sortedKeys($rows[0]),
        );
        $this->assertSame('o1', $rows[0]->getAttribute('first.$id'));
        $this->assertSame('o1', $rows[0]->getAttribute('second.$id'));
    }

    public function testSharedTablesNeverReturnTheJoinedTenant(): void
    {
        $database = $this->database(sharedTables: true);

        $rows = $database->find('customers', [
            Query::join('orders', '$id', 'customerId', '=', 'ord'),
        ]);

        $this->assertCount(1, $rows);
        $this->assertSame($this->keys('ord', '$tenant'), $this->sortedKeys($rows[0]), 'Only the main document carries a tenant');
        $this->assertArrayNotHasKey('ord.$tenant', $rows[0]->getArrayCopy());
    }

    public function testEmulatedFullOuterJoinReturnsTheSameKeysFromBothHalves(): void
    {
        $database = $this->database(sharedTables: false);

        $rows = $database->find('customers', [
            Query::fullOuterJoin('orders', '$id', 'customerId', '=', 'ord'),
            Query::orderAsc('ord.total'),
        ]);

        $this->assertCount(3, $rows);
        foreach ($rows as $row) {
            $keys = $this->sortedKeys($row);
            foreach (['ord.$id', 'ord.customerId', 'ord.secret', 'ord.total'] as $joined) {
                $this->assertContains($joined, $keys);
            }
            $this->assertSame([], \array_values(\array_diff($keys, $this->keys('ord'))), 'Unexpected keys: '.\implode(', ', $keys));
        }
        $orders = \array_map(static fn (Document $row): mixed => $row->getAttribute('ord.$id'), $rows);
        $this->assertSame([null, 'o1', 'o2'], $orders);
    }

    public function testGetDocumentWithJoinReturnsJoinedAttributesUnderTheAlias(): void
    {
        $database = $this->database(sharedTables: false);

        $document = $database->getDocument('customers', 'c1', [
            Query::leftJoin('orders', '$id', 'customerId', '=', 'ord'),
        ]);

        $this->assertSame($this->keys('ord'), $this->sortedKeys($document));
        $this->assertSame(10, $document->getAttribute('ord.total'));
    }

    public function testExplicitSelectStillReturnsJoinedInternals(): void
    {
        $database = $this->database(sharedTables: false);

        $rows = $database->find('customers', [
            Query::join('orders', '$id', 'customerId', '=', 'ord'),
            Query::select(['name', 'ord.$id', 'ord.$permissions', 'ord.$createdAt']),
        ]);

        $this->assertCount(1, $rows);
        $this->assertSame('o1', $rows[0]->getAttribute('ord.$id'));
        $this->assertSame([Permission::read(Role::any())], $rows[0]->getAttribute('ord.$permissions'));
        $this->assertIsString($rows[0]->getAttribute('ord.$createdAt'));
        $this->assertArrayNotHasKey('ord.total', $rows[0]->getArrayCopy());
    }

    /**
     * @return list<string>
     */
    private function keys(string $alias, string ...$extra): array
    {
        $keys = [
            ...self::MAIN_KEYS,
            $alias.'.$id',
            $alias.'.customerId',
            $alias.'.secret',
            $alias.'.total',
            ...$extra,
        ];
        \sort($keys);

        return $keys;
    }

    /**
     * @return list<string>
     */
    private function sortedKeys(Document $document): array
    {
        $keys = \array_map(\strval(...), \array_keys($document->getArrayCopy()));
        \sort($keys);

        return $keys;
    }

    private function database(bool $sharedTables): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('projection')
            ->setNamespace('projection_'.\uniqid())
            ->setSharedTables($sharedTables)
            ->setTenant(null);
        $database->addHook(new Permissions());
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(new Collection(
            id: 'customers',
            attributes: [Attribute::string(key: 'name', size: 64, required: true)],
            permissions: $permissions,
            documentSecurity: false,
        ));
        $database->createCollection(new Collection(
            id: 'orders',
            attributes: [
                Attribute::string(key: 'customerId', size: 64, required: true),
                Attribute::integer(key: 'total', required: true),
                Attribute::string(key: 'secret', size: 64, required: true),
            ],
            permissions: $permissions,
            documentSecurity: true,
        ));

        if ($sharedTables) {
            $database->setTenant(1);
        }
        $database->createDocument('customers', new Document(['$id' => 'c1', 'name' => 'Alice']));
        $database->createDocument('customers', new Document(['$id' => 'c2', 'name' => 'Bob']));
        $database->createDocument('orders', new Document([
            '$id' => 'o1',
            'customerId' => 'c1',
            'total' => 10,
            'secret' => 'first-secret',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument('orders', new Document([
            '$id' => 'o2',
            'customerId' => 'nobody',
            'total' => 20,
            'secret' => 'second-secret',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        return $database;
    }
}
