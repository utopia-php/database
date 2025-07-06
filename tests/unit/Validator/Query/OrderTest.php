<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\QueryContext;
use Utopia\Database\Validator\Queries\V2 as DocumentsValidator;

class OrderTest extends TestCase
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
                '$id' => '$sequence',
                'key' => '$sequence',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]),
        ]);

        $context = new QueryContext();

        $context->add($collection);

        $this->validator = new DocumentsValidator($context);
    }

    public function testValueSuccess(): void
    {
        $this->assertTrue($this->validator->isValid([Query::orderAsc('attr')]));
        $this->assertTrue($this->validator->isValid([Query::orderAsc()]));
        $this->assertTrue($this->validator->isValid([Query::orderDesc('attr')]));
        $this->assertTrue($this->validator->isValid([Query::orderDesc()]));
        $this->assertTrue($this->validator->isValid([Query::limit(101)]));
        $this->assertTrue($this->validator->isValid([Query::offset(5001)]));
        $this->assertTrue($this->validator->isValid([Query::equal('attr', ['v'])]));
    }

    public function testValueFailure(): void
    {
        $this->assertFalse($this->validator->isValid([Query::limit(-1)]));
        $this->assertFalse($this->validator->isValid([Query::limit(0)]));
        $this->assertEquals('Invalid limit: Value must be a valid range between 1 and 9,223,372,036,854,775,807', $this->validator->getDescription());
        $this->assertFalse($this->validator->isValid([Query::offset(-1)]));
        $this->assertFalse($this->validator->isValid([Query::equal('dne', ['v'])]));
        $this->assertFalse($this->validator->isValid([Query::equal('', ['v'])]));
        $this->assertFalse($this->validator->isValid([Query::orderDesc('dne')]));
        $this->assertFalse($this->validator->isValid([Query::orderAsc('dne')]));
    }
}
