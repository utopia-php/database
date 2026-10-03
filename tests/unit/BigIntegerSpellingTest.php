<?php

namespace Tests\Unit;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Redis;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ColumnType;
use ValueError;

/**
 * Every bigint attribute written before 8.0 is stored with the type `bigint`,
 * so the library keeps writing that spelling and reads `biginteger` (written
 * by pre-release builds of 8.0) as the same type.
 */
final class BigIntegerSpellingTest extends TestCase
{
    private const string PERSISTED = 'bigint';

    public function testPersistedTypeKeepsTheBigintSpellingForBigIntegerOnly(): void
    {
        foreach (ColumnType::cases() as $type) {
            $expected = $type === ColumnType::BigInteger ? self::PERSISTED : $type->value;

            $this->assertSame($expected, Attribute::persistedType($type), $type->name);
            $this->assertSame($type, Attribute::normalizeType(Attribute::persistedType($type)), $type->name);
        }
    }

    public function testBothSpellingsNormalizeToBigInteger(): void
    {
        foreach ([self::PERSISTED, ColumnType::BigInteger->value] as $spelling) {
            $this->assertSame(ColumnType::BigInteger, Attribute::normalizeType($spelling), $spelling);
            $this->assertSame(ColumnType::BigInteger, Attribute::tryNormalizeType($spelling), $spelling);
        }

        $this->assertNull(Attribute::tryNormalizeType('huge'));

        $this->expectException(ValueError::class);
        Attribute::normalizeType('huge');
    }

    public function testAttributeModelsHoldThePersistedSpelling(): void
    {
        $attribute = Attribute::bigInteger(key: 'total');

        $this->assertSame(ColumnType::BigInteger, $attribute->type);
        $this->assertSame(self::PERSISTED, $attribute->getAttribute('type'));
        $this->assertSame(self::PERSISTED, $attribute->toDocument()->getAttribute('type'));
        $this->assertSame(self::PERSISTED, (new Attribute(key: 'total', type: ColumnType::BigInteger))->getAttribute('type'));
        $this->assertSame(self::PERSISTED, Attribute::fromArray([
            '$id' => 'total',
            'type' => ColumnType::BigInteger->value,
        ])->getAttribute('type'));
        $this->assertSame(self::PERSISTED, Attribute::fromDocument(new Document([
            '$id' => 'total',
            'type' => self::PERSISTED,
        ]))->getAttribute('type'));

        $changed = Attribute::integer(key: 'count');

        $changed->type = ColumnType::BigInteger;
        $this->assertSame(self::PERSISTED, $changed->getAttribute('type'));

        $changed->setAttribute('type', ColumnType::Integer);
        $this->assertSame(ColumnType::Integer->value, $changed->getAttribute('type'));

        $changed->setAttribute('type', ColumnType::BigInteger->value);
        $this->assertSame(self::PERSISTED, $changed->getAttribute('type'));
        $this->assertSame(ColumnType::BigInteger, $changed->type);

        $changed['type'] = ColumnType::BigInteger;
        $this->assertSame(self::PERSISTED, $changed->getAttribute('type'));

        $changed->setAttribute('type', 'huge');
        $this->assertSame('huge', $changed->getAttribute('type'), 'An unknown type is stored as given so validation can report it');
    }

    /**
     * @return iterable<string, array{Closure(): Adapter}>
     */
    public static function adapters(): iterable
    {
        yield 'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))];
        yield 'memory' => [static fn (): Adapter => new Memory()];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testNewBigIntegerAttributesPersistTheBigintSpelling(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $database->createCollection(new Collection(
            id: 'ledger',
            attributes: [Attribute::bigInteger(key: 'inline')],
            permissions: $this->permissions(),
        ));
        $database->createAttribute('ledger', Attribute::bigInteger(key: 'single'));
        $database->createAttributes('ledger', [Attribute::bigInteger(key: 'batch')]);
        $database->createAttribute('ledger', Attribute::integer(key: 'widened'));
        $database->updateAttribute('ledger', 'widened', type: ColumnType::BigInteger);
        $database->updateAttribute('ledger', 'single', required: true);

        $this->assertSame([
            'inline' => self::PERSISTED,
            'single' => self::PERSISTED,
            'batch' => self::PERSISTED,
            'widened' => self::PERSISTED,
        ], $this->storedTypes($database, 'ledger'));

        foreach ($database->getCollection('ledger')->attributes as $attribute) {
            $this->assertSame(ColumnType::BigInteger, $attribute->type, $attribute->key);
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testStoredBigIntegerSpellingIsWrittenBackAsBigint(Closure $adapter): void
    {
        $database = $this->database($adapter);
        $database->createCollection(new Collection(
            id: 'ledger',
            attributes: [
                Attribute::bigInteger(key: 'total'),
                Attribute::bigInteger(key: 'untouched'),
            ],
            permissions: $this->permissions(),
        ));
        $this->storeType($database, 'ledger', 'total', ColumnType::BigInteger->value);
        $this->storeType($database, 'ledger', 'untouched', ColumnType::BigInteger->value);

        foreach ($database->getCollection('ledger')->attributes as $attribute) {
            $this->assertSame(ColumnType::BigInteger, $attribute->type, $attribute->key);
        }

        $database->updateAttributeRequired('ledger', 'total', true);

        $this->assertSame([
            'total' => self::PERSISTED,
            'untouched' => self::PERSISTED,
        ], $this->storedTypes($database, 'ledger'));
    }

    /**
     * @return iterable<string, array{Closure(): Adapter, string}>
     */
    public static function storedSpellings(): iterable
    {
        foreach (self::adapters() as $name => [$adapter]) {
            yield $name.' '.self::PERSISTED => [$adapter, self::PERSISTED];
            yield $name.' '.ColumnType::BigInteger->value => [$adapter, ColumnType::BigInteger->value];
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('storedSpellings')]
    public function testBothStoredSpellingsBehaveIdentically(Closure $adapter, string $spelling): void
    {
        $database = $this->database($adapter);
        $database->createCollection(new Collection(
            id: 'ledger',
            attributes: [Attribute::bigInteger(key: 'total')],
            permissions: $this->permissions(),
        ));
        $this->storeType($database, 'ledger', 'total', $spelling);

        $this->assertTrue($database->createIndex('ledger', Index::key(key: 'totals', attributes: ['total'])));

        $created = $database->createDocument('ledger', new Document([
            '$id' => 'balance',
            '$permissions' => $this->permissions(),
            'total' => (string) (PHP_INT_MAX - 1),
        ]));
        $this->assertSame(PHP_INT_MAX - 1, $created->getAttribute('total'));
        $this->assertSame(PHP_INT_MAX - 1, $database->getDocument('ledger', 'balance')->getAttribute('total'));

        $increased = $database->increaseDocumentAttribute('ledger', 'balance', 'total');
        $this->assertSame(PHP_INT_MAX, $increased->getAttribute('total'));
        $this->assertCount(1, $database->find('ledger', [Query::equal('total', [PHP_INT_MAX])]));

        try {
            $database->createDocument('ledger', new Document([
                '$id' => 'overflow',
                '$permissions' => $this->permissions(),
                'total' => '9223372036854775808',
            ]));
            $this->fail('A signed bigint above PHP_INT_MAX must be rejected');
        } catch (StructureException $exception) {
            $this->assertStringContainsString('total', $exception->getMessage());
        }

        $sqlite = $database->getAdapter();
        if (! $sqlite instanceof SQLite) {
            return;
        }

        $this->assertSame('BIGINT', $sqlite->getColumnType($spelling, 0));

        $columnTypes = [];
        foreach ($database->getSchemaAttributes('ledger') as $column) {
            $columnTypes[$column->getId()] = $column->getAttribute('columnType');
        }
        $this->assertSame(\strtolower($sqlite->getColumnType($spelling, 0)), $columnTypes['total'] ?? null);
    }

    public function testRedisSchemaRecordsPersistTheBigintSpelling(): void
    {
        /** @var array<string, array<string, mixed>> $hashes */
        $hashes = [];
        $client = self::createStub(Redis::class);
        $client->method('exists')->willReturnCallback(
            function (mixed $key) use (&$hashes): int {
                return \is_string($key) && isset($hashes[$key]) ? 1 : 0;
            }
        );
        $client->method('hMSet')->willReturnCallback(
            function (string $key, array $fields) use (&$hashes): bool {
                $hashes[$key] = [...($hashes[$key] ?? []), ...$fields];

                return true;
            }
        );
        $client->method('hSet')->willReturnCallback(
            function (string $key, mixed ...$fields) use (&$hashes): int {
                $field = $fields[0] ?? null;
                if (\is_string($field)) {
                    $hashes[$key][$field] = $fields[1] ?? null;
                }

                return 1;
            }
        );
        $client->method('hGet')->willReturnCallback(
            function (string $key, string $field) use (&$hashes): mixed {
                return $hashes[$key][$field] ?? false;
            }
        );

        $adapter = new RedisAdapter($client);
        $adapter->createCollection('ledger', [Attribute::bigInteger(key: 'inline')]);
        $adapter->createAttribute('ledger', Attribute::bigInteger(key: 'single'));
        $adapter->updateAttribute('ledger', Attribute::bigInteger(key: 'inline', required: true));

        $records = [];
        foreach ($hashes as $fields) {
            foreach (['attrs', 'schema'] as $field) {
                $encoded = $fields[$field] ?? null;
                if (! \is_string($encoded)) {
                    continue;
                }
                /** @var array<mixed> $decoded */
                $decoded = \json_decode($encoded, true, flags: JSON_THROW_ON_ERROR);
                $attributes = $field === 'schema' ? ($decoded['attributes'] ?? []) : $decoded;
                $this->assertIsArray($attributes);
                foreach ($attributes as $attribute) {
                    $this->assertIsArray($attribute);
                    $key = $attribute['key'] ?? null;
                    $this->assertIsString($key);
                    $records[$field.':'.$key] = $attribute['type'] ?? null;
                }
            }
        }

        $this->assertSame([
            'attrs:inline' => self::PERSISTED,
            'attrs:single' => self::PERSISTED,
            'schema:inline' => self::PERSISTED,
        ], $records);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    private function database(Closure $adapter): Database
    {
        $database = new Database($adapter(), new Cache(new None()));
        $database
            ->setDatabase('bigint_spelling')
            ->setNamespace('bigint_spelling_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->create();

        return $database;
    }

    /**
     * @return list<string>
     */
    private function permissions(): array
    {
        return [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
        ];
    }

    /**
     * @return array<string, mixed>
     */
    private function storedTypes(Database $database, string $collection): array
    {
        $types = [];
        foreach ($this->storedAttributes($database, $collection) as $attribute) {
            $key = $attribute['key'] ?? null;
            $this->assertIsString($key);
            $types[$key] = $attribute['type'] ?? null;
        }

        return $types;
    }

    private function storeType(Database $database, string $collection, string $key, string $type): void
    {
        $attributes = $this->storedAttributes($database, $collection);
        foreach ($attributes as $index => $attribute) {
            if (($attribute['key'] ?? null) === $key) {
                $attributes[$index]['type'] = $type;
            }
        }

        $database->skipFilters(fn (): Document => $database->getAuthorization()->skip(
            fn (): Document => $database->updateDocument(Database::METADATA, $collection, new Document([
                'attributes' => \json_encode($attributes, JSON_THROW_ON_ERROR),
            ])),
        ));
        $this->assertSame($type, $this->storedTypes($database, $collection)[$key] ?? null);
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function storedAttributes(Database $database, string $collection): array
    {
        $stored = $database->skipFilters(fn (): Document => $database->getAuthorization()->skip(
            fn (): Document => $database->getDocument(Database::METADATA, $collection),
        ));
        $attributes = $stored->getAttribute('attributes');
        $this->assertIsString($attributes, 'With filters skipped the stored JSON comes back as written');

        /** @var list<array<string, mixed>> $decoded */
        $decoded = \json_decode($attributes, true, flags: JSON_THROW_ON_ERROR);

        return $decoded;
    }
}
