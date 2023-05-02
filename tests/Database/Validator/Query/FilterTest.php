<?php

namespace Utopia\Tests\Validator\Query;

use Exception;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Filter;

class FilterTest extends TestCase
{
    /**
     * @throws Exception
     */
    public function testValue(): void
    {
        $validator = new Filter(
            attributes: [
                new Document([
                    '$id' => 'attr',
                    'key' => 'attr',
                    'type' => Database::VAR_STRING,
                    'array' => false,
                ]),
            ],
        );
        // Test for Success
        $this->assertEquals($validator->isValid(Query::between('attr', '1975-12-06', '2050-12-06')), true, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::isNotNull('attr')), true, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::isNull('attr')), true, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::startsWith('attr', 'super')), true, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::endsWith('attr', 'man')), true, $validator->getDescription());

        // Test for Failure
        $this->assertEquals($validator->isValid(Query::select(['attr'])), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::limit(1)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::limit(0)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::limit(100)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::limit(-1)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::limit(101)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::offset(1)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::offset(0)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::offset(5000)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::offset(-1)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::offset(5001)), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::equal('dne', ['v'])), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::equal('', ['v'])), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::orderAsc('attr')), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(Query::orderDesc('attr')), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(new Query(Query::TYPE_CURSORAFTER, values: ['asdf'])), false, $validator->getDescription());
        $this->assertEquals($validator->isValid(new Query(Query::TYPE_CURSORBEFORE, values: ['asdf'])), false, $validator->getDescription());
    }
}
