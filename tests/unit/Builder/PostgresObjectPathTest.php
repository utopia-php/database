<?php

namespace Tests\Unit\Builder;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Builder\Postgres;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;

final class PostgresObjectPathTest extends TestCase
{
    /**
     * @return array<string, array{string}>
     */
    public static function unsafePaths(): array
    {
        return [
            'quote in the last key' => ["meta.a' IN ('x') OR secret='s2' OR 'x"],
            'operator in the last key' => ["meta.a'||(select 1)||'"],
            'comment in the last key' => ["meta.a' OR 1=1 --"],
            'quote in a middle key' => ["meta.a'b.c"],
            'empty key' => ['meta..a'],
        ];
    }

    #[DataProvider('unsafePaths')]
    public function testAFilterOnAnUnsafeObjectPathIsAQueryError(string $path): void
    {
        $this->expectException(QueryException::class);

        (new Postgres())->compileFilters([$this->objectFilter(Query::equal($path, ['x']))]);
    }

    #[DataProvider('unsafePaths')]
    public function testAStatementWithAnUnsafeObjectPathInsideOrIsAQueryError(string $path): void
    {
        $this->expectException(QueryException::class);

        (new Postgres())
            ->from('docs')
            ->filter([Query::or([
                $this->objectFilter(Query::equal('meta.a', ['x'])),
                $this->objectFilter(Query::startsWith($path, 'x')),
            ])])
            ->build();
    }

    public function testAFilterOnAPlainObjectPathBindsItsValue(): void
    {
        $condition = (new Postgres())->compileFilters([$this->objectFilter(Query::equal('meta.user-info.home_city2', ['x']))]);

        $this->assertSame(['x'], $condition->bindings);
    }

    private function objectFilter(Query $query): Query
    {
        $query->setAttributeType(ColumnType::Object->value);

        return $query;
    }
}
