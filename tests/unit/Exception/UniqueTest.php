<?php

namespace Tests\Unit\Exception;

use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Database\Exception\Unique;

final class UniqueTest extends TestCase
{
    public function testLegacyMessageIsNormalized(): void
    {
        $previous = new RuntimeException('previous');
        $exception = new Unique('Unique index violation', 42, $previous);

        $this->assertSame('Document with the requested unique attributes already exists', $exception->getMessage());
        $this->assertSame(42, $exception->getCode());
        $this->assertSame($previous, $exception->getPrevious());
    }

    public function testSpecificMessageIsPreserved(): void
    {
        $exception = new Unique('Custom unique conflict');

        $this->assertSame('Custom unique conflict', $exception->getMessage());
    }
}
