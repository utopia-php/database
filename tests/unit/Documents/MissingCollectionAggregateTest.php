<?php

namespace Tests\Unit\Documents;

use Closure;
use DateTime;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Role;

final class MissingCollectionAggregateTest extends TestCase
{
    private const string MISSING = 'does_not_exist';

    /**
     * @return array<string, array{Closure(Database): (int|float)}>
     */
    public static function aggregates(): array
    {
        return [
            'count' => [static fn (Database $database): int => $database->count(self::MISSING)],
            'sum' => [static fn (Database $database): int|float => $database->sum(self::MISSING, 'value')],
        ];
    }

    /**
     * @return array<string, array{Closure(Database): (int|float), non-empty-string}>
     */
    public static function adapterAggregates(): array
    {
        return [
            'count' => [static fn (Database $database): int => $database->count(self::MISSING), 'count'],
            'sum' => [static fn (Database $database): int|float => $database->sum(self::MISSING, 'value'), 'sum'],
        ];
    }

    /**
     * @param  Closure(Database): (int|float)  $aggregate
     */
    #[DataProvider('aggregates')]
    public function testMissingCollectionThrowsNotFoundWithAuthorizationEnabled(Closure $aggregate): void
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $database
            ->setDatabase('missing_collection')
            ->setNamespace('missing_collection_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');

        $aggregate($database);
    }

    /**
     * @param  Closure(Database): (int|float)  $aggregate
     * @param  non-empty-string  $method
     */
    #[DataProvider('adapterAggregates')]
    public function testMissingCollectionNeverReachesTheAdapterWhenAuthorizationIsSkipped(Closure $aggregate, string $method): void
    {
        $adapter = $this->createMock(Adapter::class);
        $adapter->method('getSharedTables')->willReturn(false);
        $adapter->method('getTenant')->willReturn(null);
        $adapter->method('getTenantPerDocument')->willReturn(false);
        $adapter->method('getNamespace')->willReturn('');
        $adapter->method('getIdAttributeType')->willReturn('string');
        $adapter->method('getMaxUIDLength')->willReturn(36);
        $adapter->method('getMinDateTime')->willReturn(new DateTime('0000-01-01'));
        $adapter->method('getMaxDateTime')->willReturn(new DateTime('9999-12-31'));
        $adapter->method('getInternalIndexesKeys')->willReturn([]);
        $adapter->method('filter')->willReturnArgument(0);
        $adapter->method('supports')->willReturnCallback(
            static fn (Capability $capability): bool => $capability === Capability::DefinedAttributes
        );
        $adapter->method('getDocument')->willReturn(new Document());
        $adapter->expects($this->never())->method($method);

        $database = new Database($adapter, new Cache(new None()));

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');

        $database->getAuthorization()->skip(static fn (): int|float => $aggregate($database));
    }
}
