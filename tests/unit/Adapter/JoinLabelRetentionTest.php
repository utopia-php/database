<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class JoinLabelRetentionTest extends TestCase
{
    private const string NAMESPACE = 'join_label_retention';

    private const int READS = 1100;

    private const int KEPT = 1024;

    private Database $database;

    private SQLite $adapter;

    #[\Override]
    protected function setUp(): void
    {
        $this->adapter = new class (new PDO('sqlite::memory:')) extends SQLite {
            protected const int MAX_LABEL_LENGTH = 8;
        };
        $this->database = new Database($this->adapter, new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $this->database->create();

        foreach (['customers' => [Attribute::string('name', size: 64)], 'orders' => [Attribute::string('customerId', size: 64)]] as $id => $attributes) {
            $this->database->createCollection(Collection::create(
                id: $id,
                attributes: $attributes,
                permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
                documentSecurity: false,
            ));
        }

        $this->database->createDocument('customers', new Document(['$id' => 'alice', 'name' => 'Alice']));
        $this->database->createDocument('orders', new Document(['$id' => 'o1', 'customerId' => 'alice']));
    }

    public function testOverlongJoinLabelsAreNotKeptForEveryAliasAWorkerReads(): void
    {
        for ($read = 0; $read < self::READS; $read++) {
            $alias = 'customer_'.$read;
            $orders = $this->database->find('orders', [
                Query::join('customers', $alias, [Query::on('customerId', $alias.'.$id')]),
                Query::select(['$id', $alias.'.name']),
            ]);

            $this->assertCount(1, $orders);
            $this->assertSame('Alice', $orders[0]->getAttribute($alias.'.name'), 'The overlong label of read '.$read.' must be read back');
        }

        $labels = (new ReflectionProperty(SQL::class, 'joinLabels'))->getValue($this->adapter);
        $this->assertIsArray($labels);
        $this->assertLessThanOrEqual(self::KEPT, \count($labels), 'Every overlong label ever read is still held');
    }

    public function testAReadKeepsItsLabelsAfterOlderOnesAreEvicted(): void
    {
        for ($read = 0; $read < self::KEPT; $read++) {
            $this->database->find('orders', [
                Query::join('customers', 'older_'.$read, [Query::on('customerId', 'older_'.$read.'.$id')]),
                Query::select(['$id', 'older_'.$read.'.name']),
            ]);
        }

        $orders = $this->database->find('orders', [
            Query::join('customers', 'first', [Query::on('customerId', 'first.$id')]),
            Query::join('customers', 'second', [Query::on('customerId', 'second.$id')]),
            Query::select(['$id', 'first.name', 'second.name']),
        ]);

        $this->assertSame('Alice', $orders[0]->getAttribute('first.name'));
        $this->assertSame('Alice', $orders[0]->getAttribute('second.name'));
    }
}
