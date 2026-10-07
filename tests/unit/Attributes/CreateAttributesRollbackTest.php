<?php

namespace Tests\Unit\Attributes;

use PDOException;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Unit\Support\StderrCapture;
use TypeError;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;

final class CreateAttributesRollbackTest extends TestCase
{
    private const string COLLECTION = 'profiles';

    private const string PREFIX = 'Failed to persist metadata after retries and cleanup encountered errors for attributes creation: ';

    private ?RuntimeException $metadataFailure = null;

    /**
     * @var list<string>
     */
    private array $lockedColumns = [];

    /**
     * @var list<string>
     */
    private array $dropped = [];

    private bool $driverErrors = false;

    private ?TypeError $dropError = null;

    public function testCleanupErrorsForEveryColumnThatCouldNotBeDroppedFollowTheMetadataError(): void
    {
        $database = $this->database();
        $this->lockedColumns = ['nick', 'bio'];
        $this->metadataFailure = new RuntimeException('metadata store is read-only');

        $thrown = null;
        $stderr = StderrCapture::during(function () use ($database, &$thrown): void {
            try {
                $database->createAttributes(self::COLLECTION, $this->attributes('title', 'nick', 'bio'));
            } catch (DatabaseException $error) {
                $thrown = $error;
            }
        });

        $this->assertInstanceOf(DatabaseException::class, $thrown);
        $this->assertSame(
            self::PREFIX."metadata store is read-only | Cleanup errors: Column 'nick' is locked, Column 'bio' is locked",
            $thrown->getMessage()
        );
        $this->assertSame($this->metadataFailure, $thrown->getPrevious());
        $this->assertSame(['title'], $this->dropped, 'the column that could be dropped is rolled back');
        $this->assertStringContainsString("Failed to cleanup attribute 'nick' after 3 attempts: Column 'nick' is locked", $stderr);
        $this->assertStringContainsString("Failed to cleanup attribute 'bio' after 3 attempts: Column 'bio' is locked", $stderr);
        $this->assertStringNotContainsString("'title'", $stderr);
        $this->assertSame([], $this->storedKeys($database));
    }

    public function testACleanRollbackReportsOnlyTheMetadataErrorAndLetsTheCreateBeRetried(): void
    {
        $database = $this->database();
        $this->metadataFailure = new RuntimeException('metadata store is read-only');

        $thrown = null;
        $stderr = StderrCapture::during(function () use ($database, &$thrown): void {
            try {
                $database->createAttributes(self::COLLECTION, $this->attributes('title', 'nick'));
            } catch (DatabaseException $error) {
                $thrown = $error;
            }
        });

        $this->assertInstanceOf(DatabaseException::class, $thrown);
        $this->assertSame('Failed to persist metadata after retries for attributes creation: metadata store is read-only', $thrown->getMessage());
        $this->assertSame($this->metadataFailure, $thrown->getPrevious());
        $this->assertSame(['title', 'nick'], $this->dropped);
        $this->assertSame('', $stderr);
        $this->assertSame([], $this->storedKeys($database));

        $this->metadataFailure = null;
        $created = $database->createAttributes(self::COLLECTION, $this->attributes('title', 'nick'));
        $this->assertSame(['title', 'nick'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $created));
        $this->assertSame(['title', 'nick'], $this->storedKeys($database));
    }

    public function testADriverErrorWhileDroppingAColumnIsCollectedAndTheRemainingColumnsAreStillDropped(): void
    {
        $database = $this->database();
        $this->lockedColumns = ['nick'];
        $this->driverErrors = true;
        $this->metadataFailure = new RuntimeException('metadata store is read-only');

        $thrown = null;
        StderrCapture::during(function () use ($database, &$thrown): void {
            try {
                $database->createAttributes(self::COLLECTION, $this->attributes('nick', 'title'));
            } catch (\Throwable $error) {
                $thrown = $error;
            }
        });

        $this->assertInstanceOf(DatabaseException::class, $thrown);
        $this->assertSame(self::PREFIX."metadata store is read-only | Cleanup errors: SQLSTATE[55P03]: lock not available on 'nick'", $thrown->getMessage());
        $this->assertSame($this->metadataFailure, $thrown->getPrevious());
        $this->assertSame(['title'], $this->dropped, 'the columns after the failing one are still dropped');
        $this->assertSame([], $this->storedKeys($database));
    }

    public function testAnErrorWhileDroppingAColumnEscapesTheRollbackUnchanged(): void
    {
        $database = $this->database();
        $this->lockedColumns = ['nick'];
        $this->dropError = new TypeError('deleteAttribute(): Argument #2 ($id) must be of type string');
        $this->metadataFailure = new RuntimeException('metadata store is read-only');

        $thrown = null;
        StderrCapture::during(function () use ($database, &$thrown): void {
            try {
                $database->createAttributes(self::COLLECTION, $this->attributes('nick', 'title'));
            } catch (\Throwable $error) {
                $thrown = $error;
            }
        });

        $this->assertSame($this->dropError, $thrown, 'a programming error in the rollback is not folded into the metadata failure');
    }

    /**
     * @return list<Attribute>
     */
    private function attributes(string ...$keys): array
    {
        return \array_values(\array_map(static fn (string $key): Attribute => Attribute::string(key: $key, size: 32), $keys));
    }

    /**
     * @return list<string>
     */
    private function storedKeys(Database $database): array
    {
        return \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $database->getCollection(self::COLLECTION)->attributes()
        );
    }

    private function database(): Database
    {
        $database = new Database($this->adapter(), new Cache(new MemoryCache()));
        $database->setDatabase('rollback')->setNamespace('rollback_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(id: self::COLLECTION));

        return $database;
    }

    private function adapter(): Memory
    {
        $metadataFailure = fn (): ?RuntimeException => $this->metadataFailure;
        $drop = function (string $id): void {
            if (\in_array($id, $this->lockedColumns, true)) {
                throw $this->dropError ?? ($this->driverErrors
                    ? new PDOException("SQLSTATE[55P03]: lock not available on '{$id}'")
                    : new DatabaseException("Column '{$id}' is locked"));
            }

            $this->dropped[] = $id;
        };

        return new class ($metadataFailure, $drop) extends Memory {
            public function __construct(private readonly \Closure $metadataFailure, private readonly \Closure $drop)
            {
                parent::__construct();
            }

            #[\Override]
            public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
            {
                $failure = ($this->metadataFailure)();
                if ($failure instanceof RuntimeException && $collection->getId() === Database::METADATA) {
                    throw $failure;
                }

                return parent::updateDocument($collection, $id, $document, $skipPermissions);
            }

            #[\Override]
            public function deleteAttribute(string $collection, string $id): bool
            {
                ($this->drop)($id);

                return parent::deleteAttribute($collection, $id);
            }
        };
    }
}
