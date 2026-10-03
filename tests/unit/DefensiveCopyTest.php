<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Index;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\Order;

final class DefensiveCopyTest extends TestCase
{
    public function testCreateCollectionLeavesCallerDefinitionsUntouched(): void
    {
        $attributes = $this->attributes();
        $indexes = $this->indexes();
        $definitions = $this->snapshot($attributes, $indexes);

        $first = $this->database(new SQLite(new PDO('sqlite::memory:')))
            ->createCollection(new Collection(id: 'databases', attributes: $attributes, indexes: $indexes));

        $this->assertSame($definitions, $this->snapshot($attributes, $indexes), 'createCollection() rewrote the definitions it was given');

        $second = $this->database(new Memory())
            ->createCollection(new Collection(id: 'databases', attributes: $attributes, indexes: $indexes));
        $fresh = $this->database(new Memory())
            ->createCollection(new Collection(id: 'databases', attributes: $this->attributes(), indexes: $this->indexes()));

        $this->assertSame($definitions, $this->snapshot($attributes, $indexes), 'createCollection() rewrote the definitions it was given');
        $this->assertSame($this->snapshot($fresh->attributes, $fresh->indexes), $this->snapshot($second->attributes, $second->indexes));
        $this->assertSame($this->snapshot($first->attributes, $first->indexes), $this->snapshot($second->attributes, $second->indexes));

        $this->assertSame(['datetime'], $second->attributes[2]->filters);
        $this->assertSame([null], $second->indexes[0]->lengths);
        $this->assertSame([Order::Asc], $second->indexes[0]->orders);
        $this->assertSame([Database::MAX_ARRAY_INDEX_LENGTH], $second->indexes[1]->lengths);
        $this->assertSame([null], $second->indexes[1]->orders);
    }

    public function testCreateAttributeLeavesCallerAttributeUntouched(): void
    {
        $database = $this->database(new Memory());
        $database->createCollection(new Collection(id: 'events'));
        $attribute = Attribute::datetime(key: 'startsAt');
        $definition = $attribute->getArrayCopy();

        $database->createAttribute('events', $attribute);

        $this->assertSame($definition, $attribute->getArrayCopy());
        $this->assertSame(['datetime'], $database->getCollection('events')->attributes[0]->filters);
    }

    public function testCreateAttributesLeavesCallerAttributesUntouched(): void
    {
        $database = $this->database(new Memory());
        $database->createCollection(new Collection(id: 'events'));
        $attributes = [Attribute::datetime(key: 'endsAt'), Attribute::string(key: 'label', size: 32)];
        $definitions = $this->snapshot($attributes, []);

        $database->createAttributes('events', $attributes);

        $this->assertSame($definitions, $this->snapshot($attributes, []));
        $this->assertSame(['datetime'], $database->getCollection('events')->attributes[0]->filters);
    }

    public function testCreateIndexLeavesCallerIndexUntouched(): void
    {
        $database = $this->database(new Memory());
        $database->createCollection(new Collection(id: 'events', attributes: [Attribute::string(key: 'tags', size: 64, array: true)]));
        $index = Index::key(key: '_key_tags', attributes: ['tags'], lengths: [64], orders: [Order::Desc]);
        $definition = $index->getArrayCopy();

        $database->createIndex('events', $index);

        $this->assertSame($definition, $index->getArrayCopy());
        $stored = $database->getCollection('events')->indexes[0];
        $this->assertSame([Database::MAX_ARRAY_INDEX_LENGTH], $stored->lengths);
        $this->assertSame([null], $stored->orders);
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setDatabase('definitions')
            ->setNamespace('definitions_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->create();

        return $database;
    }

    /**
     * @return list<Attribute>
     */
    private function attributes(): array
    {
        return [
            Attribute::string(key: 'name', size: 256, required: true),
            Attribute::string(key: 'tags', size: 64, array: true),
            Attribute::datetime(key: 'expiresAt'),
        ];
    }

    /**
     * @return list<Index>
     */
    private function indexes(): array
    {
        return [
            Index::key(key: '_key_name', attributes: ['name'], lengths: [256], orders: [Order::Asc]),
            Index::key(key: '_key_tags', attributes: ['tags'], lengths: [64], orders: [Order::Desc]),
        ];
    }

    /**
     * @param  array<Attribute>  $attributes
     * @param  array<Index>  $indexes
     * @return array{attributes: list<array<string, mixed>>, indexes: list<array<string, mixed>>}
     */
    private function snapshot(array $attributes, array $indexes): array
    {
        return [
            'attributes' => \array_values(\array_map(static fn (Attribute $attribute): array => $attribute->getArrayCopy(), $attributes)),
            'indexes' => \array_values(\array_map(static fn (Index $index): array => $index->getArrayCopy(), $indexes)),
        ];
    }
}
