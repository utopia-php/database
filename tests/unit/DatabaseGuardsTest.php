<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Index;
use Utopia\Query\Builder\Statement;

final class DatabaseGuardsTest extends TestCase
{
    /**
     * @return array<string, array{\Closure(): Adapter}>
     */
    public static function adaptersWithoutTimeouts(): array
    {
        return [
            'memory' => [static fn (): Adapter => new Memory()],
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
        ];
    }

    public function testFromIsRefusedWithoutAQueryBuilder(): void
    {
        $database = $this->database(new Memory());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Query builder is not supported by this adapter');

        $database->getAuthorization()->skip(fn () => $database->from('anything'));
    }

    public function testSchemaIsRefusedWithoutAQueryBuilder(): void
    {
        $database = $this->database(new Memory());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Schema builder is not supported by this adapter');

        $database->schema();
    }

    public function testExecuteIsRefusedWithoutRawQueries(): void
    {
        $database = $this->database(new Memory());
        $statement = new Statement('SELECT 1', [], readOnly: true);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Raw queries are not supported by this adapter');

        $database->getAuthorization()->skip(fn () => $database->execute($statement));
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adaptersWithoutTimeouts')]
    public function testSetTimeoutIsRefusedWithoutTimeouts(\Closure $adapter): void
    {
        $database = $this->database($adapter());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support timeouts');

        $database->setTimeout(1_000);
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adaptersWithoutTimeouts')]
    public function testClearTimeoutIsRefusedWithoutTimeouts(\Closure $adapter): void
    {
        $database = $this->database($adapter());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support timeouts');

        $database->clearTimeout();
    }

    public function testGetConnectionIdIsNullWithoutAConnection(): void
    {
        $this->assertNull($this->database(new Memory())->getConnectionId());
    }

    public function testSchemaIntrospectionWithoutTheFeatureListsNothing(): void
    {
        $database = $this->database(new Memory());
        $database->create();
        $database->createCollection(Collection::create(
            id: 'introspected',
            attributes: [Attribute::string(key: 'name', size: 32)],
            indexes: [Index::key(key: 'byName', attributes: ['name'])],
        ));

        $this->assertSame([], $database->getSchemaAttributes('introspected'));
        $this->assertSame([], $database->getSchemaIndexes('introspected'));
    }

    public function testDropUnknownAttributesGetterFollowsTheSetter(): void
    {
        $database = $this->database(new Memory());

        $this->assertFalse($database->getDropUnknownAttributes());
        $this->assertTrue($database->setDropUnknownAttributes(true)->getDropUnknownAttributes());
        $this->assertFalse($database->setDropUnknownAttributes(false)->getDropUnknownAttributes());
    }

    public function testSkipRelationshipsWithoutTheHookRunsTheCallback(): void
    {
        $database = $this->database(new Memory());

        $this->assertNull($database->getRelationshipHook());
        $result = new stdClass();
        $this->assertSame($result, $database->skipRelationships(static fn (): stdClass => $result));
        $this->assertNull($database->getRelationshipHook(), 'skipping relationships does not install the hook');
    }

    public function testSkipRelationshipsExistCheckWithoutTheHookRunsTheCallback(): void
    {
        $database = $this->database(new Memory());

        $result = new stdClass();
        $this->assertSame($result, $database->skipRelationshipsExistCheck(static fn (): stdClass => $result));
        $this->assertNull($database->getRelationshipHook());
    }

    public function testRawQueryIsRefusedWithoutRawQueries(): void
    {
        $database = $this->database(new Memory());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Raw queries are not supported by this adapter');

        $database->getAuthorization()->skip(fn (): array => $database->rawQuery('SELECT 1'));
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->setDatabase('guards')->setNamespace('guards_'.\uniqid());

        return $database;
    }
}
