<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Limits;
use Utopia\Database\Database;
use Utopia\Query\Schema\ColumnType;

class LimitsStringEscalationTest extends TestCase
{
    public function testStringEscalatedStorageByteLimit(): void
    {
        $limits = new Limits(
            string: 4294967295,
            varchar: 16381,
            integer: 0,
            bigInteger: 0,
            attributes: 0,
            indexes: 0,
            defaultAttributes: 0,
            defaultIndexes: 0,
            indexLength: 0,
            uidLength: 36,
            documentSize: 0,
            minDateTime: new \DateTime('0000-01-01'),
            maxDateTime: new \DateTime('9999-12-31'),
            idType: ColumnType::Integer,
            keywords: [],
            internalIndexKeys: [],
        );

        $this->assertNull($limits->stringEscalatedStorageByteLimit(100));
        $this->assertSame(Database::MAX_TEXT_BYTES, $limits->stringEscalatedStorageByteLimit(16382));
        $this->assertSame(Database::MAX_TEXT_BYTES, $limits->stringEscalatedStorageByteLimit(65535));
        $this->assertSame(Database::MAX_MEDIUMTEXT_BYTES, $limits->stringEscalatedStorageByteLimit(65536));
        $this->assertSame(Database::MAX_TEXT_BYTES, $limits->stringEscalatedStorageByteLimit(20000));
        $this->assertSame(Database::MAX_MEDIUMTEXT_BYTES, $limits->stringEscalatedStorageByteLimit(100000));
        $this->assertSame(Database::MAX_LONGTEXT_BYTES, $limits->stringEscalatedStorageByteLimit(20000000));
    }

    public function testStringEscalatedStorageByteLimitDisabled(): void
    {
        $limits = new Limits(
            string: 4294967295,
            varchar: 16381,
            integer: 0,
            bigInteger: 0,
            attributes: 0,
            indexes: 0,
            defaultAttributes: 0,
            defaultIndexes: 0,
            indexLength: 0,
            uidLength: 36,
            documentSize: 0,
            minDateTime: new \DateTime('0000-01-01'),
            maxDateTime: new \DateTime('9999-12-31'),
            idType: ColumnType::Integer,
            keywords: [],
            internalIndexKeys: [],
            escalatedStringByteCap: false,
        );

        $this->assertNull($limits->stringEscalatedStorageByteLimit(20000));
    }
}
