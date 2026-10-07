<?php

namespace Tests\Unit;

use Closure;
use PDO;
use PHPUnit\Framework\TestCase;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Unconfirmed as UnconfirmedException;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Relationship;

/**
 * Covers schema calls whose definition write ends in Exception\Unconfirmed, as a MongoDB commit whose result could
 * not be confirmed does: the definition may be stored, so the table, column or index it describes must be kept.
 */
final class UnconfirmedSchemaChangeTest extends TestCase
{
    public function testACollectionWhoseDefinitionIsUnconfirmedKeepsItsTable(): void
    {
        $unconfirmed = 0;
        $database = $this->database($unconfirmed, collection: 'logs');

        $thrown = $this->attempt(fn (): mixed => $database->createCollection(Collection::create(id: 'logs')));

        $this->assertSame(1, $unconfirmed, 'An unconfirmed definition write must not run again');
        $this->assertTrue($database->collectionExists('logs'), 'The table of a definition that may be stored must be kept');
        $this->assertNotNull($database->findCollection('logs'));
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
    }

    public function testAnAttributeWhoseDefinitionIsUnconfirmedKeepsItsColumn(): void
    {
        $unconfirmed = 0;
        $database = $this->database($unconfirmed, collection: 'logs', key: 'count');
        $database->createCollection(Collection::create(id: 'logs'));

        $thrown = $this->attempt(fn (): Attribute => $database->createAttribute('logs', Attribute::integer(key: 'count')));

        $this->assertSame(1, $unconfirmed, 'An unconfirmed definition write must not run again');
        $this->assertTrue($this->hasSchemaAttribute($database, 'logs', 'count'), 'The column of a definition that may be stored must be kept');
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
    }

    public function testAnIndexWhoseDefinitionIsUnconfirmedKeepsItsIndex(): void
    {
        $unconfirmed = 0;
        $database = $this->database($unconfirmed, collection: 'logs', key: 'by_count');
        $database->createCollection(Collection::create(id: 'logs'));
        $database->createAttribute('logs', Attribute::integer(key: 'count'));

        $thrown = $this->attempt(fn (): Index => $database->createIndex('logs', Index::key(key: 'by_count', attributes: ['count'])));

        $this->assertSame(1, $unconfirmed, 'An unconfirmed definition write must not run again');
        $this->assertTrue($this->hasSchemaIndex($database, 'logs', 'by_count'), 'The index of a definition that may be stored must be kept');
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
    }

    public function testARelationshipWhoseDefinitionIsUnconfirmedKeepsItsColumnsAndCreatesItsIndexes(): void
    {
        $unconfirmed = 0;
        $database = $this->database($unconfirmed, collection: 'profiles', key: 'account');
        $database->createCollection(Collection::create(id: 'profiles'));
        $database->createCollection(Collection::create(id: 'accounts'));

        $thrown = $this->attempt(fn (): Relationship => $database->createRelationship('profiles', $this->profileAccount()));

        $this->assertSame(1, $unconfirmed, 'An unconfirmed definition write must not run again');
        $this->assertTrue($this->hasSchemaAttribute($database, 'profiles', 'account'), 'The columns of a relationship that may be stored must be kept');
        $this->assertTrue($this->hasSchemaAttribute($database, 'accounts', 'profile'), 'The columns of a relationship that may be stored must be kept');
        $this->assertTrue($this->hasSchemaIndex($database, 'profiles', '_index_account'), 'The indexes of a relationship that may be stored must still be created');
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
    }

    public function testARelationshipWhoseIndexDefinitionIsUnconfirmedKeepsItsColumns(): void
    {
        $unconfirmed = 0;
        $database = $this->database($unconfirmed, collection: 'profiles', key: '_index_account');
        $database->createCollection(Collection::create(id: 'profiles'));
        $database->createCollection(Collection::create(id: 'accounts'));

        $thrown = $this->attempt(fn (): Relationship => $database->createRelationship('profiles', $this->profileAccount()));

        $this->assertSame(1, $unconfirmed, 'An unconfirmed definition write must not run again');
        $this->assertTrue($this->hasSchemaAttribute($database, 'profiles', 'account'), 'The columns of a relationship that may be stored must be kept');
        $this->assertTrue($this->hasSchemaAttribute($database, 'accounts', 'profile'), 'The columns of a relationship that may be stored must be kept');
        $this->assertTrue($this->hasSchemaIndex($database, 'profiles', '_index_account'), 'The index of a definition that may be stored must be kept');
        $this->assertSame(['account'], $this->attributeKeys($database, 'profiles'));
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
    }

    /**
     * A database over an adapter that throws Exception\Unconfirmed once an outermost transaction commits the target
     * write: the first definition write of $collection that adds an attribute or index $key to it (or, without a key,
     * that writes it at all), or the same definition written again. $unconfirmed counts those commits.
     */
    private function database(int &$unconfirmed, string $collection, ?string $key = null): Database
    {
        $count = function () use (&$unconfirmed): void {
            $unconfirmed++;
        };

        $adapter = new class (new PDO('sqlite::memory:'), $count, $collection, $key) extends SQLite {
            private ?string $target = null;

            private bool $writesTarget = false;

            /**
             * @param  Closure(): void  $count
             */
            public function __construct(
                PDO $pdo,
                private readonly Closure $count,
                private readonly string $collection,
                private readonly ?string $key,
            ) {
                parent::__construct($pdo);
            }

            #[\Override]
            public function withTransaction(callable $callback): mixed
            {
                if ($this->inTransaction()) {
                    return parent::withTransaction($callback);
                }

                try {
                    $result = parent::withTransaction($callback);
                    $writesTarget = $this->writesTarget;
                } finally {
                    $this->writesTarget = false;
                }

                if ($writesTarget) {
                    ($this->count)();

                    throw new UnconfirmedException('Failed to commit transaction: the commit could not be confirmed');
                }

                return $result;
            }

            #[\Override]
            public function createDocument(Document $collection, Document $document): Document
            {
                $this->inspect($collection, $document);

                return parent::createDocument($collection, $document);
            }

            #[\Override]
            public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
            {
                $this->inspect($collection, $document);

                return parent::updateDocument($collection, $id, $document, $skipPermissions);
            }

            private function inspect(Document $collection, Document $definition): void
            {
                if ($collection->getId() !== Database::METADATA || $definition->getId() !== $this->collection) {
                    return;
                }

                $content = \json_encode([$definition->getAttribute('attributes'), $definition->getAttribute('indexes')]) ?: '';

                if ($this->target === null && ($this->key === null || $this->names($definition, $this->key))) {
                    $this->target = $content;
                }

                if ($content === $this->target) {
                    $this->writesTarget = true;
                }
            }

            private function names(Document $definition, string $key): bool
            {
                foreach (['attributes', 'indexes'] as $field) {
                    $entries = $definition->getAttribute($field, []);
                    if (\is_string($entries)) {
                        $entries = \json_decode($entries, true);
                    }

                    foreach (\is_array($entries) ? $entries : [] as $entry) {
                        if (\is_array($entry) && ($entry['$id'] ?? null) === $key) {
                            return true;
                        }
                    }
                }

                return false;
            }
        };

        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setDatabase('unconfirmed_schema')
            ->setNamespace('unconfirmed_schema_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();

        return $database;
    }

    private function profileAccount(): Relationship
    {
        return Relationship::oneToOne(
            relatedCollection: 'accounts',
            twoWay: true,
            key: 'account',
            twoWayKey: 'profile',
        );
    }

    /**
     * @return list<string>
     */
    private function attributeKeys(Database $database, string $collection): array
    {
        return \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            \array_values($database->getCollection($collection)->attributes()),
        );
    }

    private function hasSchemaAttribute(Database $database, string $collection, string $key): bool
    {
        foreach ($database->getSchemaAttributes($collection) as $attribute) {
            if ($attribute->getId() === $key) {
                return true;
            }
        }

        return false;
    }

    private function hasSchemaIndex(Database $database, string $collection, string $key): bool
    {
        foreach ($database->getSchemaIndexes($collection) as $index) {
            if (\str_contains($index->getId(), $key)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param  callable(): mixed  $operation
     */
    private function attempt(callable $operation): ?Throwable
    {
        try {
            $operation();
        } catch (Throwable $error) {
            return $error;
        }

        return null;
    }
}
