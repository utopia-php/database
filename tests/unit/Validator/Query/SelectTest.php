<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\QueryContext;
use Utopia\Database\Validator\Queries\V2 as DocumentsValidator;

class SelectTest extends TestCase
{
    protected DocumentsValidator $validator;

    /**
     * @throws Exception
     */
    public function setUp(): void
    {
        $collection = new Document([
            '$id' => Database::METADATA,
            '$collection' => Database::METADATA,
            'name' => 'movies',
            'attributes' => [],
            'indexes' => [],
        ]);

        $collection->setAttribute('attributes', [
            new Document([
                '$id' => 'attr',
                'key' => 'attr',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]),
            new Document([
                '$id' => 'artist',
                'key' => 'artist',
                'type' => Database::VAR_RELATIONSHIP,
                'array' => false,
            ]),
        ]);

        $context = new QueryContext();
        $context->add($collection);

        $this->validator = new DocumentsValidator(
            $context,
            Database::VAR_INTEGER,
        );
    }

    public function testValueSuccess(): void
    {
        $this->assertTrue($this->validator->isValid([Query::select('*'), Query::select('attr')]));
        $this->assertTrue($this->validator->isValid([Query::select('artist.name')]));
        $this->assertTrue($this->validator->isValid([Query::limit(1)]));
    }

    public function testValueFailure(): void
    {
        $this->assertEquals('Invalid query', $this->validator->getDescription());
        $this->assertFalse($this->validator->isValid([Query::select('name.artist')]));
    }
}
