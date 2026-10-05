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
use Utopia\Database\Exception\Unconfirmed as UnconfirmedException;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;

/**
 * Covers schema calls whose definition write ends in Exception\Unconfirmed, as a MongoDB commit whose result could
 * not be confirmed does: the definition may be stored, so the table, column or index it describes must be kept.
 */
final class UnconfirmedSchemaChangeTest extends TestCase
{
    public function testACollectionWhoseDefinitionIsUnconfirmedKeepsItsTable(): void
    {
        $unconfirmed = 0;
        $database = $this->database($this->unconfirmedFrom(1, $unconfirmed));

        $thrown = $this->attempt(fn (): mixed => $database->createCollection(new Collection(id: 'logs')));

        $this->assertSame(1, $unconfirmed, 'An unconfirmed definition write must not run again');
        $this->assertTrue($database->exists($database->getDatabase(), 'logs'), 'The table of a definition that may be stored must be kept');
        $this->assertFalse($database->getCollection('logs')->isEmpty());
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
    }

    public function testAnAttributeWhoseDefinitionIsUnconfirmedKeepsItsColumn(): void
    {
        $unconfirmed = 0;
        $database = $this->database($this->unconfirmedFrom(2, $unconfirmed));
        $database->createCollection(new Collection(id: 'logs'));

        $thrown = $this->attempt(fn (): bool => $database->createAttribute('logs', Attribute::integer(key: 'count')));

        $this->assertSame(1, $unconfirmed, 'An unconfirmed definition write must not run again');
        $this->assertTrue($this->hasSchemaAttribute($database, 'logs', 'count'), 'The column of a definition that may be stored must be kept');
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
    }

    public function testAnIndexWhoseDefinitionIsUnconfirmedKeepsItsIndex(): void
    {
        $unconfirmed = 0;
        $database = $this->database($this->unconfirmedFrom(3, $unconfirmed));
        $database->createCollection(new Collection(id: 'logs'));
        $database->createAttribute('logs', Attribute::integer(key: 'count'));

        $thrown = $this->attempt(fn (): bool => $database->createIndex('logs', Index::key(key: 'by_count', attributes: ['count'])));

        $this->assertSame(1, $unconfirmed, 'An unconfirmed definition write must not run again');
        $this->assertTrue($this->hasSchemaIndex($database, 'logs', 'by_count'), 'The index of a definition that may be stored must be kept');
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
    }

    public function testARelationshipWhoseDefinitionIsUnconfirmedKeepsItsColumnsAndCreatesItsIndexes(): void
    {
        $unconfirmed = 0;
        $database = $this->database($this->unconfirmedOn(3, $unconfirmed));
        $database->createCollection(new Collection(id: 'profiles'));
        $database->createCollection(new Collection(id: 'accounts'));

        $thrown = $this->attempt(fn (): bool => $database->createRelationship($this->profileAccount()));

        $this->assertSame(1, $unconfirmed, 'An unconfirmed definition write must not run again');
        $this->assertTrue($this->hasSchemaAttribute($database, 'profiles', 'account'), 'The columns of a relationship that may be stored must be kept');
        $this->assertTrue($this->hasSchemaAttribute($database, 'accounts', 'profile'), 'The columns of a relationship that may be stored must be kept');
        $this->assertTrue($this->hasSchemaIndex($database, 'profiles', '_index_account'), 'The indexes of a relationship that may be stored must still be created');
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
    }

    public function testARelationshipWhoseIndexDefinitionIsUnconfirmedKeepsItsColumns(): void
    {
        $unconfirmed = 0;
        $database = $this->database($this->unconfirmedOn(4, $unconfirmed));
        $database->createCollection(new Collection(id: 'profiles'));
        $database->createCollection(new Collection(id: 'accounts'));

        $thrown = $this->attempt(fn (): bool => $database->createRelationship($this->profileAccount()));

        $this->assertSame(1, $unconfirmed, 'An unconfirmed definition write must not run again');
        $this->assertTrue($this->hasSchemaAttribute($database, 'profiles', 'account'), 'The columns of a relationship that may be stored must be kept');
        $this->assertTrue($this->hasSchemaAttribute($database, 'accounts', 'profile'), 'The columns of a relationship that may be stored must be kept');
        $this->assertTrue($this->hasSchemaIndex($database, 'profiles', '_index_account'), 'The index of a definition that may be stored must be kept');
        $this->assertSame(['account'], $this->attributeKeys($database, 'profiles'));
        $this->assertInstanceOf(UnconfirmedException::class, $thrown);
    }

    /**
     * Commits every outermost transaction from the $first one on, then reports it unconfirmed.
     *
     * @return Closure(int): bool
     */
    private function unconfirmedFrom(int $first, int &$unconfirmed): Closure
    {
        return function (int $transaction) use ($first, &$unconfirmed): bool {
            if ($transaction < $first) {
                return false;
            }

            $unconfirmed++;

            return true;
        };
    }

    /**
     * Commits the $only outermost transaction, then reports it unconfirmed.
     *
     * @return Closure(int): bool
     */
    private function unconfirmedOn(int $only, int &$unconfirmed): Closure
    {
        return function (int $transaction) use ($only, &$unconfirmed): bool {
            if ($transaction !== $only) {
                return false;
            }

            $unconfirmed++;

            return true;
        };
    }

    /**
     * A database over an adapter that numbers its outermost transactions from 1 and, once one has committed, throws
     * Exception\Unconfirmed when $unconfirmed says so.
     *
     * @param  Closure(int): bool  $unconfirmed
     */
    private function database(Closure $unconfirmed): Database
    {
        $adapter = new class (new PDO('sqlite::memory:'), $unconfirmed) extends SQLite {
            private int $transactions = 0;

            /**
             * @param  Closure(int): bool  $unconfirmed
             */
            public function __construct(PDO $pdo, private readonly Closure $unconfirmed)
            {
                parent::__construct($pdo);
            }

            #[\Override]
            public function withTransaction(callable $callback): mixed
            {
                if ($this->inTransaction()) {
                    return parent::withTransaction($callback);
                }

                $transaction = ++$this->transactions;
                $result = parent::withTransaction($callback);

                if (($this->unconfirmed)($transaction)) {
                    throw new UnconfirmedException('Failed to commit transaction: the commit could not be confirmed');
                }

                return $result;
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
        return new Relationship(
            collection: 'profiles',
            relatedCollection: 'accounts',
            type: RelationType::OneToOne,
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
            \array_values($database->getCollection($collection)->attributes),
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
