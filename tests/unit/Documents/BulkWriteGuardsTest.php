<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\PermissionType;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

final class BulkWriteGuardsTest extends TestCase
{
    private const string COLLECTION = 'tasks';

    public function testCreatingDocumentsNeedsCreatePermission(): void
    {
        $database = $this->database(new Memory(), [Permission::read(Role::any())]);

        $this->assertRefused(AuthorizationException::class, null, fn (): int => $database->createDocuments(self::COLLECTION, [$this->task('a', 1)]));
        $this->assertSame(0, $database->count(self::COLLECTION));
    }

    public function testAnUpdateWithoutChangesUpdatesNothing(): void
    {
        $database = $this->database(new Memory());
        $database->createDocuments(self::COLLECTION, [$this->task('a', 1)]);

        $this->assertSame(0, $database->updateDocuments(self::COLLECTION, new Document()));
        $this->assertSame(1, $database->getDocument(self::COLLECTION, 'a')->getAttribute('rank'));
    }

    public function testABulkUpdateOfAMissingCollectionIsRefused(): void
    {
        $database = $this->database(new Memory());

        $this->assertRefused(DatabaseException::class, 'Collection not found', fn (): int => $database->updateDocuments('missing', new Document(['rank' => 2])));
    }

    public function testABulkUpdateWithAnInvalidQueryIsRefused(): void
    {
        $database = $this->database(new Memory());
        $database->createDocuments(self::COLLECTION, [$this->task('a', 1)]);

        $this->assertRefused(QueryException::class, 'Invalid query: Attribute not found in schema: missing', fn (): int => $database->updateDocuments(
            self::COLLECTION,
            new Document(['rank' => 2]),
            [Query::equal('missing', ['x'])],
        ));
        $this->assertSame(1, $database->getDocument(self::COLLECTION, 'a')->getAttribute('rank'));
    }

    public function testABulkUpdateWithACursorOfAnotherCollectionIsRefused(): void
    {
        $database = $this->database(new Memory());
        $database->createDocuments(self::COLLECTION, [$this->task('a', 1)]);
        $foreign = new Document([Document::ID => 'a', Document::COLLECTION => 'other']);

        $this->assertRefused(DatabaseException::class, 'Cursor document must be from the same Collection.', fn (): int => $database->updateDocuments(
            self::COLLECTION,
            new Document(['rank' => 2]),
            [Query::cursorAfter($foreign)],
        ));
    }

    public function testABulkUpdateWithALimitAboveTheBatchSizeUpdatesExactlyTheLimit(): void
    {
        $database = $this->database(new Memory());
        $database->createDocuments(self::COLLECTION, \array_map(fn (int $rank): Document => $this->task("t{$rank}", $rank), \range(1, 7)));

        $this->assertSame(5, $database->updateDocuments(
            self::COLLECTION,
            new Document(['label' => 'done']),
            [Query::orderAsc('rank'), Query::limit(5)],
            batchSize: 2,
        ));

        $labels = [];
        foreach ($database->find(self::COLLECTION, [Query::orderAsc('rank')]) as $task) {
            $labels[$task->getId()] = $task->getAttribute('label');
        }
        $this->assertSame(['t1' => 'done', 't2' => 'done', 't3' => 'done', 't4' => 'done', 't5' => 'done', 't6' => 'open', 't7' => 'open'], $labels);
    }

    public function testABulkUpdateOverAnUnreadableUpdateTimeIsRefused(): void
    {
        $database = $this->database(new Memory());
        $database->createDocuments(self::COLLECTION, [$this->task('a', 1)]);
        $this->corruptUpdateTime($database, 'a');

        $this->assertRefused(DatabaseException::class, null, fn (): mixed => $database->skipValidation(
            fn (): mixed => $database->withPreserveDates(
                true, fn (): int => $database->updateDocuments(self::COLLECTION, new Document(['rank' => 2, Document::UPDATED_AT => 'not-a-date'])),
            ),
        ));
        $this->assertSame(1, $database->getDocument(self::COLLECTION, 'a')->getAttribute('rank'));
    }

    public function testUpsertingAnUnchangedDocumentReturnsTheStoredOne(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        $created = $database->upsertDocument(self::COLLECTION, $this->task('a', 1));

        $again = $database->upsertDocument(self::COLLECTION, $this->task('a', 1));

        $this->assertSame($created->getSequence(), $again->getSequence());
        $this->assertSame($created->getUpdatedAt(), $again->getUpdatedAt());
        $this->assertSame(1, $again->getAttribute('rank'));
        $this->assertSame('open', $again->getAttribute('label'));
    }

    public function testUpsertingNothingWritesNothing(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));

        $this->assertSame(0, $database->upsertDocuments(self::COLLECTION, []));
        $this->assertSame(0, $database->upsertDocuments(self::COLLECTION, [], increase: 'rank'));
        $this->assertSame(0, $database->upsertDocuments('missing', []), 'an empty upsert reads nothing, not even the collection');
    }

    public function testAnUpsertThatCreatesNeedsCreatePermission(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')), [Permission::read(Role::any()), Permission::update(Role::any())]);

        $this->assertRefused(AuthorizationException::class, null, fn (): int => $database->upsertDocuments(self::COLLECTION, [$this->task('new', 1)]));
        $this->assertSame(0, $database->count(self::COLLECTION));
    }

    public function testAnUpsertCallbackFailureGoesToTheErrorHandlerAndTheUpsertContinues(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        $errors = [];

        $count = $database->upsertDocuments(
            self::COLLECTION,
            [$this->task('a', 1), $this->task('b', 2)],
            onNext: static function (Document $document): void {
                if ($document->getId() === 'a') {
                    throw new RuntimeException('the consumer refused a');
                }
            },
            onError: static function (Throwable $error) use (&$errors): void {
                $errors[] = $error->getMessage();
            },
        );

        $this->assertSame(2, $count);
        $this->assertSame(['the consumer refused a'], $errors);
        $this->assertSame(2, $database->count(self::COLLECTION));
    }

    public function testAnUpsertOfADocumentWithAnUnreadableUpdateTimeIsRefused(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        $database->createDocuments(self::COLLECTION, [$this->task('a', 1)]);
        $this->corruptUpdateTime($database, 'a');

        $this->assertRefused(DatabaseException::class, null, fn (): Document => $database->upsertDocument(self::COLLECTION, $this->task('a', 5)));
    }

    public function testIteratingWithoutALimitPagesTwentyFiveDocumentsAtATime(): void
    {
        /** @var list<int|null> $limits */
        $limits = [];
        $record = static function (?int $limit) use (&$limits): void {
            $limits[] = $limit;
        };
        $database = new class (new Memory(), new Cache(new None()), $record) extends Database {
            public function __construct(Adapter $adapter, Cache $cache, private readonly \Closure $record)
            {
                parent::__construct($adapter, $cache);
            }

            public function find(string $collection, array $queries = [], PermissionType $forPermission = PermissionType::Read): array
            {
                if ($collection === 'tasks') {
                    $limit = null;
                    foreach ($queries as $query) {
                        $value = $query->getValue();
                        if ($query->getMethod() === Method::Limit && \is_int($value)) {
                            $limit = $value;
                        }
                    }
                    ($this->record)($limit);
                }

                return parent::find($collection, $queries, $forPermission);
            }
        };
        $this->prepare($database);
        $database->createDocuments(self::COLLECTION, \array_map(fn (int $rank): Document => $this->task("t{$rank}", $rank), \range(1, 60)));
        /** @var list<int|null> $limits */
        $limits = [];

        $seen = [];
        foreach ($database->iterate(self::COLLECTION, [Query::orderAsc('rank')]) as $task) {
            $seen[] = $task->getAttribute('rank');
        }

        $this->assertSame(\range(1, 60), $seen);
        $this->assertSame([25, 25, 25], $limits);
    }

    private function corruptUpdateTime(Database $database, string $id): void
    {
        $collection = $database->getCollection(self::COLLECTION);
        $adapter = $database->getAdapter();
        $stored = $adapter->getDocument($collection, $id);
        $stored->setAttribute(Document::UPDATED_AT, 'not-a-date');
        $adapter->updateDocument($collection, $id, $stored, true);
    }

    /**
     * @param  class-string<Throwable>  $exception
     * @param  callable(): mixed  $write
     */
    private function assertRefused(string $exception, ?string $message, callable $write): void
    {
        $error = null;
        try {
            $write();
        } catch (Throwable $caught) {
            $error = $caught;
        }

        $this->assertInstanceOf($exception, $error);
        if ($message !== null) {
            $this->assertSame($message, $error->getMessage());
        }
    }

    private function task(string $id, int $rank): Document
    {
        return new Document([
            Document::ID => $id,
            Document::PERMISSIONS => [Permission::read(Role::any()), Permission::update(Role::any())],
            'rank' => $rank,
            'label' => 'open',
        ]);
    }

    /**
     * @param  list<string>|null  $permissions
     */
    private function database(Adapter $adapter, ?array $permissions = null): Database
    {
        return $this->prepare(new Database($adapter, new Cache(new None())), $permissions);
    }

    /**
     * @param  list<string>|null  $permissions
     */
    private function prepare(Database $database, ?array $permissions = null): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database
            ->setAuthorization($authorization)
            ->setDatabase('bulk')
            ->setNamespace('bulk_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::integer(key: 'rank'), Attribute::string(key: 'label', size: 16)],
            permissions: $permissions ?? [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
        ));

        return $database;
    }
}
