<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Filter\Callback;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class TogglesTest extends TestCase
{
    private const string COLLECTION = 'notes';

    private const string FILTER = 'shout';

    private const string PAST = '2000-01-01T00:00:00.000+00:00';

    /**
     * @return iterable<string, array{Closure(Database, bool): Database, Closure(Database): bool, bool}>
     */
    public static function toggles(): iterable
    {
        yield 'validation' => [
            static fn (Database $database, bool $value): Database => $database->setValidation($value),
            static fn (Database $database): bool => $database->isValidating(),
            true,
        ];
        yield 'filtering' => [
            static fn (Database $database, bool $value): Database => $database->setFiltering($value),
            static fn (Database $database): bool => $database->isFiltering(),
            true,
        ];
        yield 'profiling' => [
            static fn (Database $database, bool $value): Database => $database->setProfiling($value),
            static fn (Database $database): bool => $database->isProfiling(),
            false,
        ];
        yield 'preserve dates' => [
            static fn (Database $database, bool $value): Database => $database->setPreserveDates($value),
            static fn (Database $database): bool => $database->isPreservingDates(),
            false,
        ];
        yield 'preserve sequence' => [
            static fn (Database $database, bool $value): Database => $database->setPreserveSequence($value),
            static fn (Database $database): bool => $database->isPreservingSequence(),
            false,
        ];
        yield 'migrating' => [
            static fn (Database $database, bool $value): Database => $database->setMigrating($value),
            static fn (Database $database): bool => $database->isMigrating(),
            false,
        ];
        yield 'drop unknown attributes' => [
            static fn (Database $database, bool $value): Database => $database->setDropUnknownAttributes($value),
            static fn (Database $database): bool => $database->isDroppingUnknownAttributes(),
            false,
        ];
        yield 'shared tables' => [
            static fn (Database $database, bool $value): Database => $database->setSharedTables($value),
            static fn (Database $database): bool => $database->hasSharedTables(),
            false,
        ];
        yield 'tenant per document' => [
            static fn (Database $database, bool $value): Database => $database->setTenantPerDocument($value),
            static fn (Database $database): bool => $database->isTenantPerDocument(),
            false,
        ];
    }

    /**
     * @param  Closure(Database, bool): Database  $set
     * @param  Closure(Database): bool  $read
     */
    #[DataProvider('toggles')]
    public function testSetterReturnsTheDatabaseAndTheReaderFollowsIt(Closure $set, Closure $read, bool $default): void
    {
        $database = $this->database();

        $this->assertSame($default, $read($database));
        $this->assertSame($database, $set($database, ! $default));
        $this->assertSame(! $default, $read($database));
        $this->assertSame($database, $set($database, $default));
        $this->assertSame($default, $read($database));
    }

    public function testConfigurationSettersReturnTheDatabase(): void
    {
        $database = $this->database();

        $this->assertSame($database, $database->setAuthorization(new Authorization()));
        $this->assertSame($database, $database->setMaxQueryValues(10));
        $this->assertSame(10, $database->getMaxQueryValues());
        $this->assertSame($database, $database->setLocks(true));
        $this->assertSame($database, $database->setTenant(7));
        $this->assertSame(7, $database->getTenant());
    }

    /**
     * @return iterable<string, array{Closure(Database, bool, Closure(): mixed): mixed, Closure(Database): bool, bool}>
     */
    public static function scopes(): iterable
    {
        yield 'withValidation' => [
            static fn (Database $database, bool $value, Closure $callback): mixed => $database->withValidation($value, $callback),
            static fn (Database $database): bool => $database->isValidating(),
            true,
        ];
        yield 'withFiltering' => [
            static fn (Database $database, bool $value, Closure $callback): mixed => $database->withFiltering($value, $callback),
            static fn (Database $database): bool => $database->isFiltering(),
            true,
        ];
        yield 'withPreserveDates' => [
            static fn (Database $database, bool $value, Closure $callback): mixed => $database->withPreserveDates($value, $callback),
            static fn (Database $database): bool => $database->isPreservingDates(),
            false,
        ];
        yield 'withPreserveSequence' => [
            static fn (Database $database, bool $value, Closure $callback): mixed => $database->withPreserveSequence($value, $callback),
            static fn (Database $database): bool => $database->isPreservingSequence(),
            false,
        ];
    }

    /**
     * @param  Closure(Database, bool, Closure(): mixed): mixed  $scope
     * @param  Closure(Database): bool  $read
     */
    #[DataProvider('scopes')]
    public function testScopeAppliesTheGivenValueOnlyInsideTheCallback(Closure $scope, Closure $read, bool $default): void
    {
        $database = $this->database();

        $this->assertSame(! $default, $scope($database, ! $default, static fn (): bool => $read($database)));
        $this->assertSame($default, $read($database));
        $this->assertSame($default, $scope($database, $default, static fn (): bool => $read($database)));
    }

    /**
     * @param  Closure(Database, bool, Closure(): mixed): mixed  $scope
     * @param  Closure(Database): bool  $read
     */
    #[DataProvider('scopes')]
    public function testScopeCanTurnBackOnWhatTheHandleTurnedOff(Closure $scope, Closure $read, bool $default): void
    {
        $database = $this->database();
        $inner = static fn (bool $value): mixed => $scope($database, $value, static fn (): bool => $read($database));

        $this->assertSame($default, $scope($database, ! $default, static fn (): mixed => $inner($default)));
        $this->assertSame(! $default, $scope($database, $default, static fn (): mixed => $inner(! $default)));
    }

    public function testSkipValidationAndSkipFiltersTurnTheirToggleOffInsideTheCallback(): void
    {
        $database = $this->database();

        $this->assertFalse($database->skipValidation(static fn (): bool => $database->isValidating()));
        $this->assertFalse($database->skipFilters(static fn (): bool => $database->isFiltering()));
        $this->assertTrue($database->isValidating());
        $this->assertTrue($database->isFiltering());
    }

    public function testFilteringOffReadsTheStoredValue(): void
    {
        $database = $this->seeded();

        $this->assertSame('QUIET', $this->title($database));
        $this->assertSame('quiet', $database->withFiltering(false, fn (): string => $this->title($database)));
        $database->setFiltering(false);
        $this->assertSame('quiet', $this->title($database));
        $this->assertSame('QUIET', $database->withFiltering(true, fn (): string => $this->title($database)));
    }

    public function testNamedFiltersAreTurnedOffAndBackOn(): void
    {
        $database = $this->seeded();

        $this->assertSame('quiet', $database->withFiltering(false, fn (): string => $this->title($database), [self::FILTER]));
        $this->assertTrue($database->withFiltering(false, static fn (): bool => $database->isFiltering(), [self::FILTER]));
        $this->assertSame('QUIET', $database->skipFilters(
            fn (): string => $database->withFiltering(true, fn (): string => $this->title($database), [self::FILTER]),
            [self::FILTER],
        ));
    }

    public function testPreservedDatesKeepTheGivenCreationTime(): void
    {
        $database = $this->seeded();
        $create = static fn (string $id): Document => $database->createDocument(self::COLLECTION, new Document([
            '$id' => $id,
            '$createdAt' => self::PAST,
            'title' => $id,
        ]));

        $preserved = $database->withPreserveDates(true, static fn (): Document => $create('preserved'));
        $stamped = $database->withPreserveDates(false, static fn (): Document => $create('stamped'));

        $this->assertSame(self::PAST, $preserved->getCreatedAt());
        $this->assertNotSame(self::PAST, $stamped->getCreatedAt());
    }

    public function testIgnoreDuplicatesSkipsAnExistingIdOnlyInsideTheCallback(): void
    {
        $database = $this->seeded();
        $duplicate = static fn (): int => $database->createDocuments(self::COLLECTION, [new Document(['$id' => 'note', 'title' => 'again'])]);

        $this->assertSame(0, $database->ignoreDuplicates($duplicate));
        $this->assertSame('QUIET', $this->title($database));

        $this->expectException(DuplicateException::class);
        $duplicate();
    }

    public function testClearDocumentTypesKeepsTheMetadataCollection(): void
    {
        $database = $this->database();
        $database->setDocumentType(self::COLLECTION, TogglesNote::class);

        $database->clearDocumentType(self::COLLECTION);
        $this->assertNull($database->getDocumentType(self::COLLECTION));

        $database->setDocumentType(self::COLLECTION, TogglesNote::class);
        $database->clearDocumentTypes();

        $this->assertNull($database->getDocumentType(self::COLLECTION));
        $this->assertSame(Collection::class, $database->getDocumentType(Database::METADATA));
    }

    private function database(): Database
    {
        $database = new Database(new Memory(), new Cache(new None()), [new Callback(
            self::FILTER,
            static fn (mixed $value): mixed => \is_string($value) ? \strtolower($value) : $value,
            static fn (mixed $value): mixed => \is_string($value) ? \strtoupper($value) : $value,
        )]);

        return $database
            ->setAuthorization(new Authorization())
            ->setDatabase('toggles')
            ->setNamespace('toggles_'.\uniqid());
    }

    private function seeded(): Database
    {
        $database = $this->database();
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64, filters: [self::FILTER])],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'note', 'title' => 'Quiet']));

        return $database;
    }

    /**
     * @phpstan-impure
     */
    private function title(Database $database): string
    {
        $title = $database->getDocument(self::COLLECTION, 'note')->getAttribute('title');
        $this->assertIsString($title);

        return $title;
    }
}
