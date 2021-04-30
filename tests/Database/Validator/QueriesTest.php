<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Validator\QueryValidator;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Query;

class QueriesTest extends TestCase
{
    /**
     * @var array
     */
    protected $indexes = [
        [
            '$id' => 'testindex',
            'type' => 'text',
            'attributes' => [
                'title',
                'description'
            ],
            'orders' => [
                'ASC',
                'DESC'
            ],
        ],
        [
            '$id' => 'testindex2',
            'type' => 'text',
            'attributes' => [
                'title',
                'description'
            ],
            'orders' => [
                'ASC',
                'DESC'
            ],
        ],
    ];

    public function setUp(): void
    {
    }

    public function tearDown(): void
    {
    }

    public function testQueries()
    {
    }

}
