<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use Redis;
use Tests\Unit\CountingAdapterHooks;
use Tests\Unit\CountingAttribute;
use Tests\Unit\CountingCollection;
use Tests\Unit\CountingDatabase;
use Tests\Unit\CountingIndex;
use Tests\Unit\CountingRelationship;
use Tests\Unit\MagicAccessAssertions;
use Tests\Unit\MagicAccessRecorder;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Index;
use Utopia\Database\Relationship;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;
use Utopia\Query\Schema\IndexType;
use Utopia\Query\Schema\Order;

#[RequiresPhpExtension('redis')]
final class RedisSchemaMagicReadsTest extends TestCase
{
    use MagicAccessAssertions;

    private const string NAMESPACE = 'redis_schema';

    private const string DATABASE = 'redis_schema';

    private const string BOOKS = 'books';

    private const string AUTHORS = 'authors';

    private Redis $client;

    /** @var array<string, string> */
    private array $strings = [];

    /** @var array<string, array<string, true>> */
    private array $sets = [];

    /** @var array<string, array<string, string>> */
    private array $hashes = [];

    private bool $pipelining = false;

    /** @var list<mixed> */
    private array $queued = [];

    protected function setUp(): void
    {
        $this->client = $this->fakeClient();
    }

    /**
     * @return iterable<string, array{RelationType}>
     */
    public static function relationships(): iterable
    {
        foreach (RelationType::cases() as $type) {
            yield $type->value => [$type];
        }
    }

    /**
     * @return iterable<string, array{RelationType, RelationSide}>
     */
    public static function relationshipSides(): iterable
    {
        foreach (RelationType::cases() as $type) {
            foreach (RelationSide::cases() as $side) {
                yield $type->value.' '.$side->value.' side' => [$type, $side];
            }
        }
    }

    /**
     * @return iterable<string, array{IndexType}>
     */
    public static function indexes(): iterable
    {
        foreach ([IndexType::Key, IndexType::Unique] as $type) {
            yield $type->value => [$type];
        }
    }

    public function testCollectionLimits(): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $collection = CountingCollection::of(new Collection(
            id: self::BOOKS,
            attributes: $this->counting($recorder, [
                Attribute::string(key: 'title', size: 64),
                Attribute::string(key: 'tags', size: 32, array: true),
                Attribute::integer(key: 'pages', size: 8),
                Attribute::datetime(key: 'printedAt'),
            ]),
            indexes: [CountingIndex::of(Index::key(key: 'by_title', attributes: ['title'], lengths: [32]), $recorder)],
        ), $recorder);

        $recorder->start();
        $adapter->getAttributeWidth($collection);
        $attributes = $adapter->getCountOfAttributes($collection);
        $indexes = $adapter->getCountOfIndexes($collection);
        $recorder->stop();

        $this->assertGreaterThan(0, $attributes);
        $this->assertGreaterThan(0, $indexes);
        $this->assertNoMagicAccess($recorder, 'Redis getAttributeWidth(), getCountOfAttributes() and getCountOfIndexes()');
    }

    public function testCreateCollection(): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $attributes = $this->counting($recorder, [
            Attribute::string(key: 'label', size: 64, required: true),
            Attribute::string(key: 'codes', size: 32, array: true),
            Attribute::integer(key: 'capacity', size: 8, signed: false),
        ]);
        $indexes = [
            CountingIndex::of(Index::key(key: 'by_label', attributes: ['label', 'capacity'], lengths: [32], orders: [Order::Asc, Order::Desc]), $recorder),
            CountingIndex::of(Index::unique(key: 'unique_label', attributes: ['label']), $recorder),
        ];

        $recorder->start();
        $created = $adapter->createCollection('shelves', $attributes, $indexes);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, 'Redis createCollection()');
    }

    public function testCreateAttribute(): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $attribute = CountingAttribute::of(Attribute::string(key: 'subtitle', size: 128, default: 'none'), $recorder);

        $recorder->start();
        $created = $adapter->createAttribute(self::BOOKS, $attribute);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, 'Redis createAttribute()');
    }

    public function testCreateAttributes(): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $attributes = $this->counting($recorder, [
            Attribute::string(key: 'subtitle', size: 128),
            Attribute::string(key: 'labels', size: 32, array: true),
            Attribute::boolean(key: 'lent'),
        ]);

        $recorder->start();
        $created = $adapter->createAttributes(self::BOOKS, $attributes);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, 'Redis createAttributes()');
    }

    public function testUpdateAttribute(): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $attribute = CountingAttribute::of(Attribute::integer(key: 'pages', size: 8, signed: false), $recorder);

        $recorder->start();
        $updated = $adapter->updateAttribute(self::BOOKS, $attribute, 'length');
        $recorder->stop();

        $this->assertTrue($updated);
        $this->assertNoMagicAccess($recorder, 'Redis updateAttribute()');
    }

    #[DataProvider('indexes')]
    public function testCreateIndex(IndexType $type): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $index = CountingIndex::of(new Index(key: 'by_title_and_pages', type: $type, attributes: ['title', 'pages'], lengths: [32], orders: [Order::Asc, Order::Desc]), $recorder);

        $recorder->start();
        $created = $adapter->createIndex(self::BOOKS, $index, ['title' => ColumnType::String->value, 'pages' => ColumnType::Integer->value]);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, 'Redis createIndex() '.$type->value);
    }

    #[DataProvider('relationships')]
    public function testCreateRelationship(RelationType $type): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $relationship = CountingRelationship::of($this->relationship($type, RelationSide::Parent), $recorder);

        $recorder->start();
        $created = $adapter->createRelationship($relationship);
        $recorder->stop();

        $this->assertTrue($created);
        $this->assertNoMagicAccess($recorder, 'Redis createRelationship() '.$type->value);
    }

    #[DataProvider('relationships')]
    public function testUpdateRelationship(RelationType $type): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $adapter->createRelationship($this->relationship($type, RelationSide::Parent));
        $relationship = CountingRelationship::of($this->relationship($type, RelationSide::Parent), $recorder);

        $recorder->start();
        $updated = $adapter->updateRelationship($relationship, 'writer', 'works');
        $recorder->stop();

        $this->assertTrue($updated);
        $this->assertNoMagicAccess($recorder, 'Redis updateRelationship() '.$type->value);
    }

    #[DataProvider('relationshipSides')]
    public function testDeleteRelationship(RelationType $type, RelationSide $side): void
    {
        $recorder = new MagicAccessRecorder();
        $adapter = $this->adapter($recorder);
        $adapter->createRelationship($this->relationship($type, RelationSide::Parent));
        $relationship = CountingRelationship::of($this->relationship($type, $side), $recorder);

        $recorder->start();
        $deleted = $adapter->deleteRelationship($relationship);
        $recorder->stop();

        $this->assertTrue($deleted);
        $this->assertNoMagicAccess($recorder, 'Redis deleteRelationship() '.$type->value.' from the '.$side->value.' side');
    }

    public function testDatabaseCreateCollection(): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($recorder);
        $collection = CountingCollection::of(new Collection(
            id: 'shelves',
            attributes: $this->counting($recorder, [
                Attribute::string(key: 'label', size: 64, required: true),
                Attribute::datetime(key: 'installedAt'),
            ]),
            indexes: [CountingIndex::of(Index::key(key: 'by_label', attributes: ['label'], lengths: [32], orders: [Order::Desc]), $recorder)],
            permissions: $this->permissions(),
        ), $recorder);

        $recorder->start();
        $database->createCollection($collection);
        $recorder->stop();

        $this->assertSame(['label', 'installedAt'], $this->attributeKeys($database, 'shelves'));
        $this->assertNoMagicAccess($recorder, 'createCollection() over Redis');
    }

    public function testDatabaseCreateAttribute(): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($recorder);
        $attribute = CountingAttribute::of(Attribute::datetime(key: 'publishedAt'), $recorder);

        $recorder->start();
        $database->createAttribute(self::BOOKS, $attribute);
        $recorder->stop();

        $this->assertContains('publishedAt', $this->attributeKeys($database, self::BOOKS));
        $this->assertNoMagicAccess($recorder, 'createAttribute() over Redis');
    }

    public function testDatabaseUpdateAttribute(): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($recorder);

        $recorder->start();
        $database->updateAttribute(self::BOOKS, 'pages', size: 8, newKey: 'length');
        $recorder->stop();

        $this->assertContains('length', $this->attributeKeys($database, self::BOOKS));
        $this->assertNoMagicAccess($recorder, 'updateAttribute() over Redis');
    }

    public function testDatabaseCreateIndex(): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($recorder);
        $index = CountingIndex::of(Index::key(key: 'by_title_and_pages', attributes: ['title', 'pages'], lengths: [32], orders: [Order::Asc, Order::Desc]), $recorder);

        $recorder->start();
        $database->createIndex(self::BOOKS, $index);
        $recorder->stop();

        $this->assertContains('by_title_and_pages', \array_map(static fn (Index $index): string => $index->getKey(), $database->getCollection(self::BOOKS)->getIndexes()));
        $this->assertNoMagicAccess($recorder, 'createIndex() over Redis');
    }

    #[DataProvider('relationships')]
    public function testDatabaseCreateRelationship(RelationType $type): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($recorder);
        $relationship = CountingRelationship::of($this->relationship($type, RelationSide::Parent), $recorder);

        $recorder->start();
        $database->createRelationship($relationship);
        $recorder->stop();

        $this->assertContains('author', $this->attributeKeys($database, self::BOOKS));
        $this->assertNoMagicAccess($recorder, 'createRelationship() '.$type->value.' over Redis');
    }

    #[DataProvider('relationships')]
    public function testDatabaseUpdateRelationship(RelationType $type): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($recorder);
        $database->createRelationship($this->relationship($type, RelationSide::Parent));

        $recorder->start();
        $database->updateRelationship(self::BOOKS, 'author', newKey: 'writer', newTwoWayKey: 'works', onDelete: ForeignKeyAction::Cascade);
        $recorder->stop();

        $this->assertContains('writer', $this->attributeKeys($database, self::BOOKS));
        $this->assertNoMagicAccess($recorder, 'updateRelationship() '.$type->value.' over Redis');
    }

    #[DataProvider('relationshipSides')]
    public function testDatabaseDeleteRelationship(RelationType $type, RelationSide $side): void
    {
        $recorder = new MagicAccessRecorder();
        $database = $this->database($recorder);
        $database->createRelationship($this->relationship($type, RelationSide::Parent));
        [$collection, $key] = $side === RelationSide::Parent ? [self::BOOKS, 'author'] : [self::AUTHORS, 'books'];

        $recorder->start();
        $database->deleteRelationship($collection, $key);
        $recorder->stop();

        $this->assertNotContains('author', $this->attributeKeys($database, self::BOOKS));
        $this->assertNoMagicAccess($recorder, 'deleteRelationship() '.$type->value.' from the '.$side->value.' side over Redis');
    }

    private function relationship(RelationType $type, RelationSide $side): Relationship
    {
        return $side === RelationSide::Parent
            ? new Relationship(collection: self::BOOKS, relatedCollection: self::AUTHORS, type: $type, twoWay: true, key: 'author', twoWayKey: 'books', onDelete: ForeignKeyAction::SetNull, side: $side)
            : new Relationship(collection: self::AUTHORS, relatedCollection: self::BOOKS, type: $type, twoWay: true, key: 'books', twoWayKey: 'author', onDelete: ForeignKeyAction::SetNull, side: $side);
    }

    /**
     * @param  list<Attribute>  $attributes
     * @return list<Attribute>
     */
    private function counting(MagicAccessRecorder $recorder, array $attributes): array
    {
        return \array_map(static fn (Attribute $attribute): Attribute => CountingAttribute::of($attribute, $recorder), $attributes);
    }

    /**
     * @return list<string>
     */
    private function permissions(): array
    {
        return [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())];
    }

    /**
     * @return list<string>
     */
    private function attributeKeys(Database $database, string $collection): array
    {
        return \array_map(
            static fn (Attribute $attribute): string => $attribute->getKey(),
            \array_values($database->getCollection($collection)->getDeclaredAttributes()),
        );
    }

    private function adapter(MagicAccessRecorder $recorder): RedisAdapter
    {
        $authorization = new Authorization();
        $authorization->disable();
        $adapter = new class ($this->client) extends RedisAdapter {
            use CountingAdapterHooks;
        };
        $adapter->setAuthorization($authorization);
        $adapter->setDatabase(self::DATABASE);
        $adapter->setNamespace(self::NAMESPACE);
        $adapter->create(self::DATABASE);
        $adapter->createCollection(self::BOOKS, [Attribute::string(key: 'title', size: 64), Attribute::integer(key: 'pages')]);
        $adapter->createCollection(self::AUTHORS, [Attribute::string(key: 'name', size: 64)]);
        $adapter->countHookReads($recorder);

        return $adapter;
    }

    private function database(MagicAccessRecorder $recorder): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $adapter = new class ($this->client) extends RedisAdapter {
            use CountingAdapterHooks;
        };
        $adapter->countHookReads($recorder);

        $database = new CountingDatabase($adapter, new Cache(new None()), $recorder);
        $database
            ->setAuthorization($authorization)
            ->setDatabase(self::DATABASE)
            ->setNamespace(self::NAMESPACE)
            ->enableValidation();
        $database->create();
        $database->addHook(new Relationships($database));
        $database->createCollection(new Collection(
            id: self::BOOKS,
            attributes: [Attribute::string(key: 'title', size: 64, required: true), Attribute::integer(key: 'pages')],
            permissions: $this->permissions(),
        ));
        $database->createCollection(new Collection(
            id: self::AUTHORS,
            attributes: [Attribute::string(key: 'name', size: 64)],
            permissions: $this->permissions(),
        ));

        return $database;
    }

    private function fakeClient(): Redis
    {
        $client = self::createStub(Redis::class);
        $client->method('ping')->willReturn(true);
        $client->method('multi')->willReturnCallback(function () use ($client): Redis {
            $this->pipelining = true;
            $this->queued = [];

            return $client;
        });
        $client->method('exec')->willReturnCallback(function (): mixed {
            $replies = $this->queued;
            $this->pipelining = false;
            $this->queued = [];

            return $replies;
        });
        $client->method('discard')->willReturnCallback(function (): bool {
            $this->pipelining = false;
            $this->queued = [];

            return true;
        });
        $client->method('get')->willReturnCallback(fn (string $key): mixed => $this->reply($client, $this->strings[$key] ?? false));
        $client->method('mGet')->willReturnCallback(fn (mixed $keys): mixed => $this->reply($client, \array_map(
            fn (mixed $key): string|false => $this->strings[$this->text($key)] ?? false,
            \is_array($keys) ? \array_values($keys) : [],
        )));
        $client->method('set')->willReturnCallback(function (string $key, mixed $value) use ($client): mixed {
            $this->forget($key);
            $this->strings[$key] = $this->text($value);

            return $this->reply($client, true);
        });
        $client->method('incr')->willReturnCallback(function (string $key, int $by = 1) use ($client): mixed {
            $value = (int) ($this->strings[$key] ?? 0) + $by;
            $this->strings[$key] = (string) $value;

            return $this->reply($client, $value);
        });
        $client->method('exists')->willReturnCallback(fn (mixed ...$keys): mixed => $this->reply(
            $client,
            \count(\array_filter($keys, fn (mixed $key): bool => $this->has($this->text($key)))),
        ));
        $client->method('del')->willReturnCallback(function (mixed $key, mixed ...$otherKeys) use ($client): mixed {
            $removed = 0;
            foreach ([...(\is_array($key) ? \array_values($key) : [$key]), ...$otherKeys] as $candidate) {
                $candidate = $this->text($candidate);
                $removed += (int) $this->has($candidate);
                $this->forget($candidate);
            }

            return $this->reply($client, $removed);
        });
        $client->method('sAdd')->willReturnCallback(function (string $key, mixed ...$members) use ($client): mixed {
            $added = 0;
            foreach ($members as $member) {
                $member = $this->text($member);
                $added += (int) ! isset($this->sets[$key][$member]);
                $this->sets[$key][$member] = true;
            }

            return $this->reply($client, $added);
        });
        $client->method('sRem')->willReturnCallback(function (string $key, mixed ...$members) use ($client): mixed {
            $removed = 0;
            foreach ($members as $member) {
                $member = $this->text($member);
                $removed += (int) isset($this->sets[$key][$member]);
                unset($this->sets[$key][$member]);
            }
            if (($this->sets[$key] ?? null) === []) {
                unset($this->sets[$key]);
            }

            return $this->reply($client, $removed);
        });
        $client->method('sMembers')->willReturnCallback(fn (string $key): mixed => $this->reply($client, $this->members($key)));
        $client->method('sIsMember')->willReturnCallback(fn (string $key, mixed $member): mixed => $this->reply($client, isset($this->sets[$key][$this->text($member)])));
        $client->method('sCard')->willReturnCallback(fn (string $key): mixed => $this->reply($client, \count($this->sets[$key] ?? [])));
        $client->method('sUnion')->willReturnCallback(fn (string ...$keys): mixed => $this->reply(
            $client,
            \array_values(\array_unique(\array_merge(...\array_map($this->members(...), $keys)))),
        ));
        $client->method('hSet')->willReturnCallback(function (string $key, string $field, mixed $value) use ($client): mixed {
            $added = (int) ! isset($this->hashes[$key][$field]);
            $this->hashes[$key][$field] = $this->text($value);

            return $this->reply($client, $added);
        });
        $client->method('hMSet')->willReturnCallback(function (string $key, mixed $fields) use ($client): mixed {
            foreach (\is_array($fields) ? $fields : [] as $field => $value) {
                $this->hashes[$key][(string) $field] = $this->text($value);
            }

            return $this->reply($client, true);
        });
        $client->method('hGet')->willReturnCallback(fn (string $key, string $field): mixed => $this->reply($client, $this->hashes[$key][$field] ?? false));
        $client->method('hGetAll')->willReturnCallback(fn (string $key): mixed => $this->reply($client, $this->hashes[$key] ?? []));
        $client->method('hDel')->willReturnCallback(function (string $key, string ...$fields) use ($client): mixed {
            $removed = 0;
            foreach ($fields as $field) {
                $removed += (int) isset($this->hashes[$key][$field]);
                unset($this->hashes[$key][$field]);
            }
            if (($this->hashes[$key] ?? null) === []) {
                unset($this->hashes[$key]);
            }

            return $this->reply($client, $removed);
        });
        $client->method('rawCommand')->willThrowException(new \RedisException('MEMORY USAGE is not available'));
        $client->method('type')->willReturnCallback(fn (string $key): int => match (true) {
            isset($this->strings[$key]) => Redis::REDIS_STRING,
            isset($this->sets[$key]) => Redis::REDIS_SET,
            isset($this->hashes[$key]) => Redis::REDIS_HASH,
            default => Redis::REDIS_NOT_FOUND,
        });
        $client->method('scan')->willReturnCallback(fn (mixed $iterator, ?string $pattern = null): mixed => $this->keys($pattern ?? '*'));

        return $client;
    }

    private function reply(Redis $client, mixed $value): mixed
    {
        if (! $this->pipelining) {
            return $value;
        }
        $this->queued[] = $value;

        return $client;
    }

    private function text(mixed $value): string
    {
        return \is_scalar($value) ? (string) $value : '';
    }

    /**
     * @return list<string>
     */
    private function members(string $key): array
    {
        return \array_map($this->text(...), \array_keys($this->sets[$key] ?? []));
    }

    /**
     * @return list<string>
     */
    private function keys(string $pattern): array
    {
        $keys = \array_map($this->text(...), [...\array_keys($this->strings), ...\array_keys($this->sets), ...\array_keys($this->hashes)]);

        return \array_values(\array_filter($keys, static fn (string $key): bool => \fnmatch($pattern, $key)));
    }

    private function has(string $key): bool
    {
        return isset($this->strings[$key]) || isset($this->sets[$key]) || isset($this->hashes[$key]);
    }

    private function forget(string $key): void
    {
        unset($this->strings[$key], $this->sets[$key], $this->hashes[$key]);
    }
}
