<?php

namespace Tests\Unit;

use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ColumnType;

final class BigIntegerTest extends TestCase
{
    public function testSqlColumnTypesMapBigIntegerAndLegacyMetadata(): void
    {
        $mariaDB = new MariaDB(new \stdClass());
        $postgres = new Postgres(new \stdClass());

        $this->assertSame('BIGINT', $mariaDB->getColumnType(ColumnType::BigInteger->value, 0));
        $this->assertSame('BIGINT', $mariaDB->getColumnType('bigint', 9999));
        $this->assertSame('BIGINT UNSIGNED', $mariaDB->getColumnType(ColumnType::BigInteger->value, 0, false));
        $this->assertSame('BIGINT', $mariaDB->getColumnType(ColumnType::BigSerial->value, 0));
        $this->assertSame('BIGINT', $postgres->getColumnType(ColumnType::BigInteger->value, 0));
        $this->assertSame('BIGINT', $postgres->getColumnType('bigint', 9999));
        $this->assertSame('BIGSERIAL', $postgres->getColumnType(ColumnType::BigSerial->value, 0));
    }

    public function testCastingNormalizesLegacyBigIntegerWithoutPrecisionLoss(): void
    {
        /** @var Adapter&Stub $adapter */
        $adapter = self::createStub(Adapter::class);
        $adapter->method('supports')->willReturnCallback(
            static fn (Capability $capability): bool => $capability === Capability::Casting
        );
        $database = new Database($adapter, new Cache(new None()));
        $collection = new Document([
            'attributes' => [
                [
                    '$id' => 'signed',
                    'type' => 'bigint',
                    'array' => false,
                    'signed' => true,
                ],
                [
                    '$id' => 'unsigned',
                    'type' => ColumnType::BigInteger->value,
                    'array' => false,
                    'signed' => false,
                ],
            ],
        ]);
        $document = new Document([
            'signed' => '9223372036854775807',
            'unsigned' => '18446744073709551615',
        ]);

        $result = $database->casting($collection, $document);

        $this->assertSame(PHP_INT_MAX, $result->getAttribute('signed'));
        $this->assertSame('18446744073709551615', $result->getAttribute('unsigned'));
    }

    public function testSQLiteCreatesAndReadsBigIntegerColumn(): void
    {
        $adapter = new SQLite(new \PDO('sqlite::memory:'));
        $adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);
        $collection = new Document([
            '$id' => 'bigints',
            'attributes' => [new Document([
                '$id' => 'value',
                'type' => ColumnType::BigInteger->value,
                'array' => false,
            ])],
        ]);

        $this->assertTrue($adapter->createCollection('bigints', [
            new Attribute(key: 'value', type: ColumnType::BigInteger),
        ]));
        $this->assertInstanceOf(Document::class, $adapter->createDocument($collection, new Document([
            '$id' => 'maximum',
            '$permissions' => [],
            'value' => PHP_INT_MAX,
        ])));

        $stored = $adapter->getDocument($collection, 'maximum')->getAttribute('value');
        $this->assertTrue(\is_int($stored) || \is_string($stored));
        $this->assertSame((string) PHP_INT_MAX, (string) $stored);
    }
}
