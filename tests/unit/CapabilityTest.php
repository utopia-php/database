<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Capability;
use Utopia\Database\Exception\NotSupported;

class CapabilityTest extends TestCase
{
    public function testCapabilityEnumValues(): void
    {
        $this->assertEquals('schemas', Capability::Schemas->value);
        $this->assertEquals('attributes', Capability::Attributes->value);
        $this->assertEquals('fulltextIndex', Capability::FulltextIndex->value);
        $this->assertEquals('vectors', Capability::Vectors->value);
        $this->assertEquals('relationships', Capability::Relationships->value);
        $this->assertEquals('ttlIndexes', Capability::TTLIndexes->value);
    }

    public function testCapabilityFromValue(): void
    {
        $this->assertEquals(Capability::Schemas, Capability::from('schemas'));
        $this->assertEquals(Capability::FulltextIndex, Capability::from('fulltextIndex'));
        $this->assertEquals(Capability::TTLIndexes, Capability::from('ttlIndexes'));
    }

    public function testCapabilityCases(): void
    {
        $cases = Capability::cases();
        $this->assertGreaterThanOrEqual(48, count($cases));
    }

    public function testNotSupportedExceptionWithAdapter(): void
    {
        $exception = new NotSupported(Capability::FulltextIndex->value, 'SQLite');
        $this->assertStringContainsString('fulltextIndex', $exception->getMessage());
        $this->assertStringContainsString('SQLite', $exception->getMessage());
    }

    public function testNotSupportedExceptionWithoutAdapter(): void
    {
        $exception = new NotSupported(Capability::Vectors->value);
        $this->assertStringContainsString('vectors', $exception->getMessage());
        $this->assertStringNotContainsString('adapter', $exception->getMessage());
    }

    public function testNotSupportedExceptionIsInstanceOfDatabaseException(): void
    {
        $exception = new NotSupported(Capability::Schemas->value, 'Mongo');
        $this->assertInstanceOf(\Utopia\Database\Exception::class, $exception);
    }
}
