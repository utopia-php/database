<?php

namespace Tests\Unit\Documents;

use ArrayObject;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Interceptor;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\WriteContext;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;

/**
 * Under ignoreDuplicates() a batch document whose id already exists is not written. Its
 * permissions must not be written either: a read grant lands in `_perms`, which find(),
 * count() and sum() consult, so the existing document would become readable by every role
 * the replayed copy names. Each case runs with RETURNING and with the read-back that engines
 * without RETURNING (MySQL) use.
 */
final class SkipDuplicatesPermissionTest extends TestCase
{
    private const string NAMESPACE = 'skip_duplicates_permission';

    private const string COLLECTION = 'notes';

    private const string RANK = 'rank';

    private const string SLUG = 'slug';

    private const string EXISTING = 'existing';

    private const string FRESH = 'fresh';

    private const int TENANT = 5;

    private const int OTHER_TENANT = 6;

    private const string ALICE = 'alice';

    private PDO $pdo;

    private Authorization $authorization;

    private Database $database;

    /**
     * @return array<string, array{bool, bool, bool}>
     */
    public static function modes(): array
    {
        $modes = [];
        foreach (self::returning() as $name => [$returning]) {
            $modes['dedicated tables, '.$name] = [false, false, $returning];
            $modes['shared tables, '.$name] = [true, false, $returning];
            $modes['tenant per document, '.$name] = [true, true, $returning];
        }

        return $modes;
    }

    /**
     * @return array<string, array{bool}>
     */
    public static function returning(): array
    {
        return [
            'returning' => [true],
            'read-back' => [false],
        ];
    }

    #[DataProvider('modes')]
    public function testASkippedDuplicateGrantsNothingOnTheExistingDocument(bool $sharedTables, bool $tenantPerDocument, bool $returning): void
    {
        $this->open($sharedTables, $tenantPerDocument, $returning);
        $this->database->createDocument(self::COLLECTION, $this->note(self::EXISTING, Role::user(self::ALICE), 5));

        $recorder = $this->recordCreatedDocuments();
        /** @var ArrayObject<int, string> $emitted */
        $emitted = new ArrayObject();
        $created = $this->database->ignoreDuplicates(fn (): int => $this->database->createDocuments(
            self::COLLECTION,
            [
                $this->note(self::EXISTING, Role::any(), 7),
                $this->note(self::FRESH, Role::any(), 3),
            ],
            onNext: static function (Document $document) use ($emitted): void {
                $emitted->append($document->getId());
            },
        ));

        $this->assertSame([['read', 'user:'.self::ALICE]], $this->grants(self::EXISTING), 'The existing document keeps its own grants');
        $this->assertSame([self::FRESH], $this->readableIds(), 'A guest must not find the existing document');
        $this->assertSame(1, $this->read(fn (): int => $this->database->count(self::COLLECTION)), 'A guest must not count the existing document');
        $this->assertSame(3, $this->read(fn (): int|float => $this->database->sum(self::COLLECTION, self::RANK)), 'A guest must not sum the existing document');

        $this->assertSame(1, $created, 'Only the inserted document is counted as created');
        $this->assertSame([self::FRESH], $emitted->getArrayCopy(), 'Only the inserted document is handed to onNext');
        $this->assertSame([[self::FRESH]], $recorder->created, 'Write hooks see only the inserted documents');

        $this->authorization->addRole(Role::user(self::ALICE)->toString());
        $this->assertSame([self::EXISTING, self::FRESH], $this->readableIds());
        $this->assertSame(5, $this->read(fn (): Document => $this->database->getDocument(self::COLLECTION, self::EXISTING))->getAttribute(self::RANK));
    }

    #[DataProvider('modes')]
    public function testARepeatedIdInOneBatchIsWrittenWithTheFirstCopysGrants(bool $sharedTables, bool $tenantPerDocument, bool $returning): void
    {
        $this->open($sharedTables, $tenantPerDocument, $returning);

        $recorder = $this->recordCreatedDocuments();
        $created = $this->database->ignoreDuplicates(fn (): int => $this->database->createDocuments(
            self::COLLECTION,
            [
                $this->note(self::FRESH, Role::user(self::ALICE), 1),
                $this->note(self::FRESH, Role::any(), 2),
            ],
        ));

        $this->assertSame([['read', 'user:'.self::ALICE]], $this->grants(self::FRESH), 'The second copy of the id must not add its grants');
        $this->assertSame([], $this->readableIds());
        $this->assertSame(1, $created);
        $this->assertSame([[self::FRESH]], $recorder->created);
    }

    #[DataProvider('modes')]
    public function testADocumentSkippedForAnotherUniqueValueGrantsNothing(bool $sharedTables, bool $tenantPerDocument, bool $returning): void
    {
        $this->open($sharedTables, $tenantPerDocument, $returning);
        $this->database->createDocument(self::COLLECTION, $this->note(self::EXISTING, Role::user(self::ALICE), 5));

        $recorder = $this->recordCreatedDocuments();
        $created = $this->database->ignoreDuplicates(fn (): int => $this->database->createDocuments(
            self::COLLECTION,
            [
                $this->note(self::FRESH, Role::any(), 3, slug: self::EXISTING),
                $this->note('other', Role::any(), 4),
            ],
        ));

        $this->assertSame([], $this->grants(self::FRESH), 'A document the engine skipped leaves no grants behind');
        $this->assertSame(['other'], $this->readableIds());
        $this->assertSame(1, $created);
        $this->assertSame([['other']], $recorder->created);

        $this->expectException(DuplicateException::class);
        $this->database->createDocument(self::COLLECTION, $this->note(self::FRESH, Role::any(), 3, slug: self::EXISTING));
    }

    #[DataProvider('modes')]
    public function testARepeatedIdWhoseFirstCopyIsSkippedIsNotWrittenFromTheSecond(bool $sharedTables, bool $tenantPerDocument, bool $returning): void
    {
        $this->open($sharedTables, $tenantPerDocument, $returning);
        $this->database->createDocument(self::COLLECTION, $this->note(self::EXISTING, Role::user(self::ALICE), 5));

        $recorder = $this->recordCreatedDocuments();
        $created = $this->database->ignoreDuplicates(fn (): int => $this->database->createDocuments(
            self::COLLECTION,
            [
                $this->note(self::FRESH, Role::any(), 3, slug: self::EXISTING),
                $this->note(self::FRESH, Role::user(self::ALICE), 4),
            ],
        ));

        $this->assertSame([], $this->grants(self::FRESH), 'The skipped first copy must not lend its grants to a row of the second');
        $this->assertSame([], $this->readableIds());
        $this->assertSame(0, $created, 'Of a repeated id only the first copy is written, and it was skipped');
        $this->assertSame([], $recorder->created);
    }

    #[DataProvider('returning')]
    public function testTheSameIdUnderAnotherTenantIsStillInserted(bool $returning): void
    {
        $this->open(true, true, $returning);
        $this->database->createDocument(self::COLLECTION, $this->note(self::EXISTING, Role::user(self::ALICE), 5));

        $recorder = $this->recordCreatedDocuments();
        $created = $this->database->ignoreDuplicates(fn (): int => $this->database->createDocuments(
            self::COLLECTION,
            [
                $this->note(self::EXISTING, Role::any(), 7),
                $this->note(self::EXISTING, Role::any(), 9, self::OTHER_TENANT),
            ],
        ));

        $this->assertSame([], $this->readableIds(self::TENANT), 'The existing document keeps its own grants');
        $this->assertSame([self::EXISTING], $this->readableIds(self::OTHER_TENANT), 'The id is new under the other tenant');
        $this->assertSame(1, $created);
        $this->assertSame([[self::EXISTING]], $recorder->created);
    }

    public function testTheMemoryAdapterCountsOnlyInsertedDocuments(): void
    {
        $this->authorization = new Authorization();
        $this->database = (new Database(new Memory(), new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE);
        $this->database->create();
        $this->database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::integer(key: self::RANK),
                Attribute::string(key: self::SLUG, size: 64),
            ],
            indexes: [Index::unique(key: self::SLUG, attributes: [self::SLUG])],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));
        $this->database->createDocument(self::COLLECTION, $this->note(self::EXISTING, Role::user(self::ALICE), 5));

        /** @var ArrayObject<int, string> $emitted */
        $emitted = new ArrayObject();
        $created = $this->database->ignoreDuplicates(fn (): int => $this->database->createDocuments(
            self::COLLECTION,
            [
                $this->note(self::EXISTING, Role::any(), 7),
                $this->note('other', Role::any(), 4, slug: self::EXISTING),
                $this->note(self::FRESH, Role::any(), 3),
                $this->note(self::FRESH, Role::any(), 2),
            ],
            onNext: static function (Document $document) use ($emitted): void {
                $emitted->append($document->getId());
            },
        ));

        $this->assertSame([self::FRESH], $this->readableIds());
        $this->assertSame(1, $created);
        $this->assertSame([self::FRESH], $emitted->getArrayCopy());
    }

    private function open(bool $sharedTables, bool $tenantPerDocument, bool $returning): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->authorization = new Authorization();
        $adapter = $returning ? new SQLite($this->pdo) : new class ($this->pdo) extends SQLite {
            #[\Override]
            protected function supportsInsertReturning(): bool
            {
                return false;
            }
        };

        $this->database = (new Database($adapter, new Cache(new None())))
            ->setAuthorization($this->authorization)
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setSharedTables($sharedTables)
            ->setTenant($sharedTables && ! $tenantPerDocument ? self::TENANT : null)
            ->setTenantPerDocument($tenantPerDocument)
            ->addHook(new Permissions());
        $this->database->create();
        $this->database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::integer(key: self::RANK),
                Attribute::string(key: self::SLUG, size: 64),
            ],
            indexes: [Index::unique(key: self::SLUG, attributes: [self::SLUG])],
            permissions: [Permission::create(Role::any())],
            documentSecurity: true,
        ));
    }

    /**
     * @return object{created: list<list<string>>}
     */
    private function recordCreatedDocuments(): object
    {
        $recorder = new class () extends Interceptor {
            /** @var list<list<string>> */
            public array $created = [];

            public function afterDocumentCreate(string $collection, array $documents, WriteContext $context): void
            {
                $this->created[] = \array_map(static fn (Document $document): string => $document->getId(), \array_values($documents));
            }
        };
        $this->database->addHook($recorder);

        return $recorder;
    }

    private function note(string $id, Role $reader, int $rank, int $tenant = self::TENANT, ?string $slug = null): Document
    {
        $note = new Document([
            '$id' => $id,
            '$permissions' => [Permission::read($reader)],
            self::RANK => $rank,
            self::SLUG => $slug ?? $id,
        ]);

        if ($this->database->isTenantPerDocument()) {
            $note->setAttribute('$tenant', $tenant);
        }

        return $note;
    }

    /**
     * @return array<string>
     */
    private function readableIds(int $tenant = self::TENANT): array
    {
        return $this->read(fn (): array => \array_map(
            static fn (Document $document): string => $document->getId(),
            $this->database->find(self::COLLECTION, [Query::orderAsc('$id')]),
        ), $tenant);
    }

    /**
     * Under tenant per document no tenant is selected, and a read has to name one.
     *
     * @template T
     *
     * @param  callable(): T  $read
     * @return T
     */
    private function read(callable $read, int $tenant = self::TENANT): mixed
    {
        return $this->database->isTenantPerDocument() ? $this->database->withTenant($tenant, $read) : $read();
    }

    /**
     * @return list<array{string, string}>
     */
    private function grants(string $document): array
    {
        $statement = $this->pdo->prepare(
            'SELECT "'.Storage::PERM_TYPE.'", "'.Storage::PERM_PERMISSION.'" FROM "'.self::NAMESPACE.'_'.self::COLLECTION.'_perms"'
            .' WHERE "'.Storage::PERM_DOCUMENT.'" = ? ORDER BY "'.Storage::PERM_PERMISSION.'"'
        );
        $statement->execute([$document]);

        $grants = [];
        foreach ($statement->fetchAll(PDO::FETCH_NUM) as $row) {
            $this->assertIsArray($row);
            [$type, $permission] = $row;
            $this->assertIsString($type);
            $this->assertIsString($permission);
            $grants[] = [$type, $permission];
        }

        return $grants;
    }
}
