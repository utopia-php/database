<?php

namespace Utopia\Tests\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Select;

class SelectTest extends TestCase
{
    public function testValue(): void
    {
        $validator = new Select(
            attributes: [
                new Document([
                    'key' => 'attr',
                    'type' => Database::VAR_STRING,
                    'array' => false,
                ]),
            ],
        );

        // Test for Success
        $this->assertEquals($validator->isValid(Query::select(['*', 'attr'])), true, $validator->getDescription());

        // Test for Failure
        $this->assertEquals($validator->isValid(Query::limit(1)), false, $validator->getDescription());
    }
}
