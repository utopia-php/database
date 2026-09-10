<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Database\Storage;

final class MongoIdentifierTest extends TestCase
{
    public function testReplaceCharsStringifiesObjectSequence(): void
    {
        $sequence = new class () {
            public function __toString(): string
            {
                return '507f1f77bcf86cd799439011';
            }
        };

        $adapter = new class () extends Mongo {
            public function __construct()
            {
            }
        };

        $replaceChars = new ReflectionMethod($adapter, 'replaceChars');
        $result = $replaceChars->invoke($adapter, '_', '$', [
            Storage::SEQUENCE => $sequence,
            Storage::UID => 'movies',
        ]);
        $this->assertIsArray($result);

        $this->assertSame('507f1f77bcf86cd799439011', $result[Document::SEQUENCE]);
        $this->assertSame('movies', $result[Document::ID]);
        $this->assertArrayNotHasKey(Storage::SEQUENCE, $result);
    }
}
