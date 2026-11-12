<?php

namespace Tests\Unit\Builder;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Builder\Postgres;
use Utopia\Query\Builder\JoinBuilder;
use Utopia\Query\Query;

final class PostgresIdCollationTest extends TestCase
{
    /**
     * @return array<string, array{bool, string, string}>
     */
    public static function filters(): array
    {
        return [
            'id' => [false, '_uid', '"_uid" COLLATE utf8_ci_ai IN (?)'],
            'shared id' => [true, '_uid', '"_uid" COLLATE utf8_ci_ai IN (?)'],
            'permission document' => [false, '_document', '"_document" COLLATE utf8_ci_ai IN (?)'],
            'shared permission document' => [true, '_document', '"_document" IN (?)'],
            'plain column' => [false, 'name', '"name" IN (?)'],
        ];
    }

    #[DataProvider('filters')]
    public function testEqualComparesInTheIndexCollation(bool $sharedTables, string $column, string $expected): void
    {
        $sql = (new Postgres($sharedTables))->from('docs')->filter([Query::equal($column, ['abc'])])->build()->query;

        $this->assertSame('SELECT * FROM "docs" WHERE '.$expected, $sql);
    }

    /**
     * @return array<string, array{string, string}>
     */
    public static function joins(): array
    {
        return [
            'id to id' => ['b._uid', '"a"."_uid" COLLATE utf8_ci_ai = "b"."_uid"'],
            'id to relationship key' => ['b.authorId', '"a"."_uid" = "b"."authorId"'],
        ];
    }

    #[DataProvider('joins')]
    public function testJoinOnTwoIdsComparesInTheIndexCollation(string $right, string $expected): void
    {
        $sql = (new Postgres())->from('a', 'a')->join('b', 'a._uid', $right, '=', 'b')->build()->query;

        $this->assertSame('SELECT * FROM "a" AS "a" JOIN "b" AS "b" ON '.$expected, $sql);
    }

    #[DataProvider('joins')]
    public function testJoinBuilderOnTwoIdsComparesInTheIndexCollation(string $right, string $expected): void
    {
        $sql = (new Postgres())
            ->from('a', 'a')
            ->joinWhere('b', static function (JoinBuilder $join) use ($right): void {
                $join->on('a._uid', $right);
            }, alias: 'b')
            ->build()
            ->query;

        $this->assertSame('SELECT * FROM "a" AS "a" JOIN "b" AS "b" ON '.$expected, $sql);
    }
}
