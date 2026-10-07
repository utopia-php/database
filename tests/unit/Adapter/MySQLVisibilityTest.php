<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\MySQL;

final class MySQLVisibilityTest extends TestCase
{
    /**
     * @return iterable<string, array{string, bool}>
     */
    public static function methods(): iterable
    {
        yield 'the capability list is public' => ['capabilities', true];
        yield 'the spatial column type is not public' => ['getSpatialSQLType', false];
    }

    #[DataProvider('methods')]
    public function testOnlyThePublicSurfaceIsCallable(string $method, bool $public): void
    {
        $mysql = new MySQL(new stdClass());

        $this->assertSame($public, \is_callable([$mysql, $method]));
    }
}
