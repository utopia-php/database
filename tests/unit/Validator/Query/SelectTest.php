<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Base;
use Utopia\Database\Validator\Query\Select;

class SelectTest extends TestCase
{
    protected Base|null $validator = null;

    /**
     * @throws Exception
     */
    public function setUp(): void
    {
        $this->validator = new Select(
            attributes: [
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
            ],
        );
    }

    public function testValueSuccess(): void
    {
        $this->assertTrue($this->validator->isValid(Query::select(['*', 'attr'])));
        $this->assertTrue($this->validator->isValid(Query::select(['artist.name'])));
    }

    public function testValueFailure(): void
    {
        $this->assertFalse($this->validator->isValid(Query::limit(1)));
        $this->assertSame('Invalid query', $this->validator->getDescription());
        $this->assertFalse($this->validator->isValid(Query::select(['name.artist'])));
    }
}
