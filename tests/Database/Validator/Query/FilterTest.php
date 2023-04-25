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
        $this->validator = new Filter(
            attributes: [
                new Document([
                    'key' => 'attr',
                    'type' => Database::VAR_STRING,
                    'array' => false,
                ]),
            ],
        );
        // Test for Success
        $this->assertEquals($this->validator->isValid(Query::between('attr', '1975-12-06', '2050-12-06')), true, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::isNotNull('attr')), true, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::isNull('attr')), true, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::startsWith('attr', 'super')), true, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::endsWith('attr', 'man')), true, $this->validator->getDescription());

        // Test for Failure
        $this->assertEquals($this->validator->isValid(Query::select(['attr'])), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::limit(1)), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::limit(0)), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::limit(100)), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::limit(-1)), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::limit(101)), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::offset(1)), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::offset(0)), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::offset(5000)), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::offset(-1)), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::offset(5001)), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::equal('dne', ['v'])), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::equal('', ['v'])), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::orderAsc('attr')), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(Query::orderDesc('attr')), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(new Query(Query::TYPE_CURSORAFTER, values: ['asdf'])), false, $this->validator->getDescription());
        $this->assertEquals($this->validator->isValid(new Query(Query::TYPE_CURSORBEFORE, values: ['asdf'])), false, $this->validator->getDescription());
    }
}
