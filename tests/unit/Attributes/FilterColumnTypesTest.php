<?php

namespace Tests\Unit\Attributes;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;

final class FilterColumnTypesTest extends TestCase
{
    public function testTheStringTypedNameIsGoneSoStaleComparisonsFailLoudly(): void
    {
        $this->assertFalse(\defined(Database::class.'::ATTRIBUTE_FILTER_TYPES'));
        $this->assertTrue(\defined(Database::class.'::ATTRIBUTE_FILTER_COLUMN_TYPES'));
    }
}
