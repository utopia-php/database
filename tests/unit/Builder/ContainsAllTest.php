<?php

namespace Tests\Unit\Builder;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Builder\Filtering;
use Utopia\Database\Builder\MariaDB;
use Utopia\Database\Builder\MySQL;
use Utopia\Database\Builder\Postgres;
use Utopia\Database\Query;

final class ContainsAllTest extends TestCase
{
    public function testContainsAllOnAStringMatchesAnyValueAsAWholePattern(): void
    {
        foreach ([[new MariaDB(), 'LIKE'], [new MySQL(), 'LIKE'], [new Postgres(), 'ILIKE']] as [$builder, $like]) {
            /** @var Filtering $builder */
            $condition = $builder->compileFilters([Query::containsAll('title', ['alpha', 'be%ta'])]);

            $this->assertStringContainsString(" {$like} ? OR ", $condition->expression, $builder::class);
            $this->assertStringNotContainsString(' AND ', $condition->expression, $builder::class);
            $this->assertSame(['alpha', 'be%ta'], $condition->bindings, $builder::class);
        }
    }
}
