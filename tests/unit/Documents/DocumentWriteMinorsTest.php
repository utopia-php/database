<?php

namespace Tests\Unit\Documents;

use DateTime;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;

final class DocumentWriteMinorsTest extends TestCase
{
    private const string COLLECTION = 'minors';

    private const string WRAPPED = 'wrapped';

    public function testCaseOnlyRenameStoresTheNewCasing(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $adapter = new class ($pdo) extends SQLite
        {
            /**
             * @param  Query[]  $queries
             */
            public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                return parent::getDocument($collection, \strtolower($id), $queries, $forUpdate);
            }
        };
        $database = $this->database($adapter);
        $database->createDocument(self::COLLECTION, new Document([
            '$id' => 'abc',
            'name' => 'renamed',
        ]));

        $renamed = $database->updateDocument(self::COLLECTION, 'ABC', new Document(['$id' => 'ABC']));

        $this->assertSame('ABC', $renamed->getId());
        $this->assertSame(
            [['_uid' => 'ABC', 'name' => 'renamed']],
            $this->rows($pdo, 'SELECT _uid, name FROM "'.$database->getNamespace().'_'.self::COLLECTION.'"'),
        );
    }

    public function testFindLeavesTheCallersCursorUnchanged(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        foreach (['first', 'second'] as $id) {
            $database->createDocument(self::COLLECTION, new Document([
                '$id' => $id,
                'secret' => $id,
                'seen' => '2026-01-02T03:04:05.678+00:00',
            ]));
        }
        $cursor = $database->getDocument(self::COLLECTION, 'first');
        $before = $cursor->getArrayCopy();

        $page = $database->find(self::COLLECTION, [Query::cursorAfter($cursor), Query::limit(1)]);

        $this->assertSame(['second'], \array_map(static fn (Document $document): string => $document->getId(), $page));
        $this->assertSame($before, $cursor->getArrayCopy());
    }

    public function testBulkUpdateInsideARequestTimestampComparesTheStoredTimestamp(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'first', 'counter' => 1]));
        $requestTimestamp = new DateTime();
        \usleep(5_000);

        $modified = $database->withRequestTimestamp(
            $requestTimestamp,
            fn (): int => $database->updateDocuments(self::COLLECTION, new Document(['counter' => 2])),
        );

        $this->assertSame(1, $modified);
        $this->assertSame(2, $database->getDocument(self::COLLECTION, 'first')->getAttribute('counter'));
    }

    public function testBulkUpdateOfADocumentWrittenAfterTheRequestTimestampConflicts(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'first', 'counter' => 1]));

        try {
            $database->withRequestTimestamp(
                new DateTime('-1 hour'),
                fn (): int => $database->updateDocuments(self::COLLECTION, new Document(['counter' => 2])),
            );
            $this->fail('A bulk update of a document written after the request timestamp was accepted');
        } catch (ConflictException $exception) {
            $this->assertSame('Document was updated after the request timestamp', $exception->getMessage());
        }

        $this->assertSame(1, $database->getDocument(self::COLLECTION, 'first')->getAttribute('counter'));
    }

    public function testBulkUpdateHandsOnNextDecodedValuesWithASelect(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'first', 'name' => 'one', 'counter' => 1, 'data' => ['k' => 1]]));
        /** @var list<Document> $handed */
        $handed = [];

        $database->updateDocuments(
            self::COLLECTION,
            new Document(['data' => ['k' => 2]]),
            [Query::select(['counter'])],
            onNext: function (Document $document) use (&$handed): void {
                $handed[] = $document;
            },
        );

        $this->assertCount(1, $handed);
        $this->assertSame(['k' => 2], $handed[0]->getAttribute('data'));
        $this->assertSame(1, $handed[0]->getAttribute('counter'));
        $this->assertFalse($handed[0]->offsetExists('name'));
        $this->assertSame(['k' => 2], $database->getDocument(self::COLLECTION, 'first')->getAttribute('data'));
    }

    public function testRetriedBulkUpdateHandsOnNextDecodedValues(): void
    {
        $adapter = new class (new PDO('sqlite::memory:')) extends SQLite
        {
            public int $commitFailures = 0;

            public function commitTransaction(): bool
            {
                if ($this->commitFailures > 0) {
                    $this->commitFailures--;

                    throw new RuntimeException('Commit failed');
                }

                return parent::commitTransaction();
            }
        };
        $database = $this->database($adapter);
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'first', 'counter' => 1, 'secret' => 'alpha']));
        /** @var list<Document> $handed */
        $handed = [];
        $adapter->commitFailures = 1;

        $modified = $database->updateDocuments(
            self::COLLECTION,
            new Document(['counter' => 2]),
            onNext: function (Document $document) use (&$handed): void {
                $handed[] = $document;
            },
        );

        $this->assertSame(0, $adapter->commitFailures);
        $this->assertSame(1, $modified);
        $this->assertCount(1, $handed);
        $this->assertSame('alpha', $handed[0]->getAttribute('secret'));
        $this->assertSame(2, $handed[0]->getAttribute('counter'));
        $stored = $database->getDocument(self::COLLECTION, 'first');
        $this->assertSame('alpha', $stored->getAttribute('secret'));
        $this->assertSame(2, $stored->getAttribute('counter'));
    }

    /**
     * @return array<string, array{list<Query>}>
     */
    public static function bulkUpdateSelections(): array
    {
        return [
            'without a select' => [[]],
            'with a select that leaves the updated attribute out' => [[Query::select(['counter'])]],
        ];
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('bulkUpdateSelections')]
    public function testBulkUpdateHandsOnNextUpdatedValuesDecodedOnce(array $queries): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'first', 'counter' => 1, 'secret' => 'alpha']));
        /** @var list<Document> $handed */
        $handed = [];

        $database->updateDocuments(
            self::COLLECTION,
            new Document(['secret' => 'gamma']),
            $queries,
            onNext: function (Document $document) use (&$handed): void {
                $handed[] = $document;
            },
        );

        $this->assertCount(1, $handed);
        $this->assertSame('gamma', $handed[0]->getAttribute('secret'));
        $this->assertSame(1, $handed[0]->getAttribute('counter'));
        $this->assertSame('gamma', $database->getDocument(self::COLLECTION, 'first')->getAttribute('secret'));
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function rows(PDO $pdo, string $sql): array
    {
        $statement = $pdo->query($sql);
        $this->assertNotFalse($statement);

        /** @var list<array<string, mixed>> $rows */
        $rows = $statement->fetchAll(PDO::FETCH_ASSOC);

        return $rows;
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()), [
            self::WRAPPED => [
                'encode' => static fn (mixed $value): ?string => $value === null ? null : \json_encode(['value' => $value], JSON_THROW_ON_ERROR),
                'decode' => static function (mixed $value): mixed {
                    if ($value === null) {
                        return null;
                    }

                    $decoded = \is_string($value) ? \json_decode($value, true) : null;
                    if (! \is_array($decoded) || ! \array_key_exists('value', $decoded)) {
                        throw new RuntimeException('Decoded a value that was never encoded: '.\var_export($value, true));
                    }

                    return $decoded['value'];
                },
            ],
        ]);
        $database->addHook(new Permissions());
        $database
            ->setDatabase('write_minors')
            ->setNamespace('write_minors_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::string('name', size: 64, required: false),
                Attribute::integer('counter', required: false),
                Attribute::string('secret', size: 1024, required: false, filters: [self::WRAPPED]),
                Attribute::string('data', size: 1024, required: false, filters: ['json']),
                Attribute::datetime('seen', required: false, filters: ['datetime']),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: true,
        ));

        return $database;
    }
}
