<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Validator\Sequence;
use Utopia\Query\Schema\ColumnType;

final class SequenceTest extends TestCase
{
    private const string UUID7 = '0192f4b8-3c1e-7a2b-9c4d-5e6f7a8b9c0d';

    public function testAcceptsAUuid7(): void
    {
        $this->assertTrue((new Sequence(ColumnType::Uuid7->value, true))->isValid(self::UUID7));
    }

    public function testRejectsAUuid7WithATrailingNewline(): void
    {
        $this->assertFalse((new Sequence(ColumnType::Uuid7->value, true))->isValid(self::UUID7."\n"));
    }
}
