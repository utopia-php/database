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
        yield 'the spatial column type is not public' => ['getSpatialSqlType', false];
    }

    #[DataProvider('methods')]
    public function testOnlyThePublicSurfaceIsCallable(string $method, bool $public): void
    {
        $mysql = new MySQL(new stdClass());

        $this->assertTrue(\method_exists($mysql, $method), "MySQL no longer declares {$method}, so its visibility is not under test");
        $this->assertSame($public, \is_callable([$mysql, $method]));
    }
}
