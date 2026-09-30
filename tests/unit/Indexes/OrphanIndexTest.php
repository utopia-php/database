<?php

namespace Tests\Unit\Indexes;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Validator\Authorization;

/**
 * SQLite names its physical indexes after the namespace, tenant and collection. The adapter
 * here reports them under their index id, as MariaDB and MySQL do, where one physical index
 * of a shared table serves every tenant's collection.
 */
final class OrphanIndexTest extends TestCase
{
    private const string COLLECTION = 'people';

    private const string INDEX = 'lookup';

    private string $path;

    private string $namespace;

    protected function setUp(): void
    {
        $path = \tempnam(\sys_get_temp_dir(), 'orphan_index_');
        $this->assertIsString($path);
        $this->path = $path;
        $this->namespace = 'orphan_index_'.\uniqid();
    }

    protected function tearDown(): void
    {
        if (\is_file($this->path)) {
            \unlink($this->path);
        }
    }

    public function testCreateIndexReplacesAMismatchedOrphanIndex(): void
    {
        $database = $this->database();
        $database->getAdapter()->createIndex(self::COLLECTION, Index::key(key: self::INDEX, attributes: ['name']));

        $this->assertTrue($database->createIndex(self::COLLECTION, Index::unique(key: self::INDEX, attributes: ['email'])));

        $this->assertSame([['email'], 0], $this->schemaIndex($database));
        $database->createDocument(self::COLLECTION, new Document(['email' => 'user@example.com']));
        $this->expectException(DuplicateException::class);
        $database->createDocument(self::COLLECTION, new Document(['email' => 'user@example.com']));
    }

    public function testCreateIndexReusesAMatchingOrphanIndex(): void
    {
        $database = $this->database();
        $database->getAdapter()->createIndex(self::COLLECTION, Index::key(key: self::INDEX, attributes: ['name']));

        $this->assertTrue($database->createIndex(self::COLLECTION, Index::key(key: self::INDEX, attributes: ['name'])));

        $this->assertSame([['name'], 1], $this->schemaIndex($database));
        $this->assertSame([self::INDEX], $this->indexKeys($database));
    }

    public function testSharedTablesRefuseAnotherTenantsIndexOfAnotherDefinition(): void
    {
        $first = $this->database(tenant: 1);
        $first->createIndex(self::COLLECTION, Index::key(key: self::INDEX, attributes: ['name']));
        $second = $this->database(tenant: 2);

        try {
            $second->createIndex(self::COLLECTION, Index::unique(key: self::INDEX, attributes: ['email']));
            $this->fail('An index another tenant uses with another definition must be refused');
        } catch (DuplicateException $error) {
            $this->assertSame('Index exists in the shared table with another definition', $error->getMessage());
        }

        $this->assertSame([['_tenant', 'name'], 1], $this->schemaIndex($first));
        $this->assertSame([], $this->indexKeys($second));
    }

    public function testSharedTablesReuseAnotherTenantsIndexOfTheSameDefinition(): void
    {
        $first = $this->database(tenant: 1);
        $first->createIndex(self::COLLECTION, Index::key(key: self::INDEX, attributes: ['name']));
        $second = $this->database(tenant: 2);

        $this->assertTrue($second->createIndex(self::COLLECTION, Index::key(key: self::INDEX, attributes: ['name'])));

        $this->assertSame([['_tenant', 'name'], 1], $this->schemaIndex($first));
        $this->assertSame([self::INDEX], $this->indexKeys($second));
    }

    private function database(?int $tenant = null): Database
    {
        $database = new Database($this->adapter(), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('orphan_index')
            ->setNamespace($this->namespace)
            ->setSharedTables($tenant !== null)
            ->setTenant($tenant);
        $database->getAuthorization()->addRole(Role::any()->toString());

        if (! $database->exists()) {
            $database->create();
        }

        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::string(key: 'email', size: 64),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
            documentSecurity: false,
        ));

        return $database;
    }

    private function adapter(): SQLite
    {
        return new class (new PDO('sqlite:'.$this->path)) extends SQLite {
            public function getSchemaIndexes(string $collection): array
            {
                $prefix = '/^'.\preg_quote($this->getNamespace(), '/').'_[^_]*_'.\preg_quote($this->filter($collection), '/').'_/';

                return \array_map(
                    fn (Document $index): Document => $index->setAttribute(Document::ID, \preg_replace($prefix, '', $index->getId()) ?? $index->getId()),
                    parent::getSchemaIndexes($collection),
                );
            }
        };
    }

    /**
     * @return array{list<string>, int}
     */
    private function schemaIndex(Database $database): array
    {
        foreach ($database->getSchemaIndexes(self::COLLECTION) as $index) {
            if ($index->getId() === self::INDEX) {
                /** @var list<string> $columns */
                $columns = $index->getAttribute('columns');
                /** @var int $nonUnique */
                $nonUnique = $index->getAttribute('nonUnique');

                return [$columns, $nonUnique];
            }
        }

        $this->fail('The index is not in the schema');
    }

    /**
     * @return list<string>
     */
    private function indexKeys(Database $database): array
    {
        return \array_map(
            static fn (Index $index): string => $index->key,
            \array_values($database->getCollection(self::COLLECTION)->indexes),
        );
    }
}
