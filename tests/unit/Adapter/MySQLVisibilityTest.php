<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\MySQL;

final class MySQLVisibilityTest extends TestCase
{
    public function testTheSpatialColumnTypeIsNotPublic(): void
    {
        $mysql = new MySQL(new stdClass());

        $this->assertTrue(\is_callable([$mysql, 'capabilities']));
        $this->assertFalse(\is_callable([$mysql, 'getSpatialSQLType']));
    }
}
