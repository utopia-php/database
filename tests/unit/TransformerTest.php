<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;

class TransformerTest extends TestCase
{
    public function testSetTransformer(): void
    {
        $database = $this->createMock(Database::class);

        // Create a reflection to access the protected property
        $reflection = new \ReflectionClass(Database::class);
        $property = $reflection->getProperty('transformer');
        $property->setAccessible(true);

        // Initially null
        $this->assertNull($property->getValue($database));
    }

    public function testSetAndGetTransformer(): void
    {
        // Create a mock that allows calling the real methods
        $database = $this->getMockBuilder(Database::class)
            ->disableOriginalConstructor()
            ->onlyMethods([])
            ->getMock();

        $transformer = function (Document $document, Document $collection, Database $db): Document {
            $document->setAttribute('transformed', true);
            return $document;
        };

        $result = $database->setTransformer($transformer);

        // Test fluent interface
        $this->assertInstanceOf(Database::class, $result);

        // Test getter
        $this->assertSame($transformer, $database->getTransformer());
    }

    public function testGetTransformerReturnsNullByDefault(): void
    {
        $database = $this->getMockBuilder(Database::class)
            ->disableOriginalConstructor()
            ->onlyMethods([])
            ->getMock();

        $this->assertNull($database->getTransformer());
    }

    public function testSetTransformerToNull(): void
    {
        $database = $this->getMockBuilder(Database::class)
            ->disableOriginalConstructor()
            ->onlyMethods([])
            ->getMock();

        $transformer = function (Document $document, Document $collection, Database $db): Document {
            return $document;
        };

        $database->setTransformer($transformer);
        $this->assertNotNull($database->getTransformer());

        $database->setTransformer(null);
        $this->assertNull($database->getTransformer());
    }

    public function testTransformerMethodChaining(): void
    {
        $database = $this->getMockBuilder(Database::class)
            ->disableOriginalConstructor()
            ->onlyMethods([])
            ->getMock();

        $transformer1 = function (Document $document, Document $collection, Database $db): Document {
            $document->setAttribute('first', true);
            return $document;
        };

        $transformer2 = function (Document $document, Document $collection, Database $db): Document {
            $document->setAttribute('second', true);
            return $document;
        };

        // Test chaining
        $database
            ->setTransformer($transformer1)
            ->setTransformer($transformer2);

        $this->assertSame($transformer2, $database->getTransformer());
    }
}
