<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Document;
use Utopia\Database\Storage;
use Utopia\Mongo\Client;

final class MongoIdentifierTest extends TestCase
{
    public function testGetDocumentReadsAnObjectSequenceAsItsString(): void
    {
        $row = new stdClass();
        $row->{Storage::SEQUENCE} = new class () {
            public function __toString(): string
            {
                return '507f1f77bcf86cd799439011';
            }
        };
        $row->{Storage::UID} = 'movies';

        $client = new class ($row) extends Client {
            public function __construct(private readonly stdClass $row)
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }

            /**
             * @param  array<mixed>  $filters
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function find(string $collection, array $filters = [], array $options = []): stdClass
            {
                return (object) ['cursor' => (object) ['firstBatch' => [$this->row], 'id' => 0]];
            }
        };

        $document = (new Mongo($client))->getDocument(new Document(['$id' => 'movies']), 'movies');

        $this->assertSame('507f1f77bcf86cd799439011', $document->getAttribute(Document::SEQUENCE));
        $this->assertSame('movies', $document->getId());
        $this->assertArrayNotHasKey(Storage::SEQUENCE, $document->getArrayCopy());
    }
}
