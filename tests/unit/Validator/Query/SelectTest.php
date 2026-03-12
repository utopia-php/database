<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Base;
use Utopia\Database\Validator\Query\Select;
use Utopia\Query\Schema\ColumnType;

class SelectTest extends TestCase
{
    protected ?Base $validator = null;

    /**
     * @throws Exception
     */
    protected function setUp(): void
    {
        $this->validator = new Select(
            attributes: [
                new Document([
                    '$id' => 'attr',
                    'key' => 'attr',
                    'type' => ColumnType::String->value,
                    'array' => false,
                ]),
                new Document([
                    '$id' => 'artist',
                    'key' => 'artist',
                    'type' => ColumnType::Relationship->value,
                    'array' => false,
                ]),
            ],
        );
    }

    public function test_value_success(): void
    {
        $this->assertTrue($this->validator->isValid(Query::select(['*', 'attr'])));
        $this->assertTrue($this->validator->isValid(Query::select(['artist.name'])));
    }

    public function test_value_failure(): void
    {
        $this->assertFalse($this->validator->isValid(Query::limit(1)));
        $this->assertEquals('Invalid query', $this->validator->getDescription());
        $this->assertFalse($this->validator->isValid(Query::select(['name.artist'])));
    }
}
