<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;

final class ConvertQueriesCapabilityTest extends TestCase
{
    private const string COLLECTION = 'books';

    public function testAnAdapterWithoutObjectsIsNotAskedForDefinedAttributes(): void
    {
        $adapter = new class () extends Memory {
            public int $definedAttributesAsked = 0;

            #[\Override]
            public function capabilities(): array
            {
                return \array_values(\array_filter(parent::capabilities(), static fn (Capability $capability): bool => $capability !== Capability::Objects));
            }

            #[\Override]
            public function supports(Capability $feature): bool
            {
                if ($feature === Capability::DefinedAttributes) {
                    $this->definedAttributesAsked++;
                }

                return parent::supports($feature);
            }
        };
        $database = $this->database($adapter);
        $collection = $database->getCollection(self::COLLECTION);
        $adapter->definedAttributesAsked = 0;

        $converted = $database->convertQueries($collection, [Query::equal('title', ['Dune'])]);
        $database->convertQuery($collection, Query::equal('title', ['Dune']));

        $this->assertSame(0, $adapter->definedAttributesAsked);
        $this->assertSame(['Dune'], $converted[0]->getValues());
        $this->assertSame(1, $database->count(self::COLLECTION, [Query::equal('title', ['Dune'])]));
    }

    public function testAnAdapterWithObjectsStillConvertsAPathIntoAnObject(): void
    {
        $database = $this->database(new Memory());
        $database->createAttribute(self::COLLECTION, Attribute::object(key: 'meta'));
        $collection = $database->getCollection(self::COLLECTION);

        [$converted] = $database->convertQueries($collection, [Query::equal('meta.level', ['x'])]);

        $this->assertSame(ColumnType::Object->value, $converted->getAttributeType());
        $this->assertSame(ColumnType::Object->value, $database->convertQuery($collection, Query::equal('meta.level', ['x']))->getAttributeType());
    }

    private function database(Memory $adapter): Database
    {
        $database = new Database($adapter, new Cache(new MemoryCache()));
        $database->setDatabase('convert')->setNamespace('convert_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'dune', 'title' => 'Dune']));

        return $database;
    }
}
