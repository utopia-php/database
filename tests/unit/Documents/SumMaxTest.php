<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Utopia\Database\Database;
use Utopia\Database\Exception\Query as QueryException;

final class SumMaxTest extends TestCase
{
    /**
     * @return iterable<string, array{Closure(Database, int): mixed, int}>
     */
    public static function nonPositiveMaxima(): iterable
    {
        $sum = static fn (Database $database, int $max): mixed => $database->sum(HookFixture::COLLECTION, 'views', max: $max);
        $count = static fn (Database $database, int $max): mixed => $database->count(HookFixture::COLLECTION, max: $max);

        yield 'sum with a zero max' => [$sum, 0];
        yield 'sum with a negative max' => [$sum, -1];
        yield 'count with a zero max' => [$count, 0];
        yield 'count with a negative max' => [$count, -1];
    }

    /**
     * @param  Closure(Database, int): mixed  $aggregate
     */
    #[DataProvider('nonPositiveMaxima')]
    public function testANonPositiveMaxIsRefused(Closure $aggregate, int $max): void
    {
        $database = $this->database();

        try {
            $aggregate($database, $max);
            $this->fail('A max of '.$max.' was accepted');
        } catch (QueryException $error) {
            $this->assertSame('Max must be greater than 0', $error->getMessage());
        }
    }

    public function testAPositiveMaxCapsTheDocumentsRead(): void
    {
        $database = $this->database();

        $this->assertSame(2, $database->count(HookFixture::COLLECTION, max: 2));
        $this->assertSame(3, $database->sum(HookFixture::COLLECTION, 'views', max: 2));
    }

    public function testNoMaxReadsEveryDocument(): void
    {
        $database = $this->database();

        $this->assertSame(3, $database->count(HookFixture::COLLECTION));
        $this->assertSame(6, $database->sum(HookFixture::COLLECTION, 'views'));
    }

    private function database(): Database
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['first', 'second', 'third']);

        return $database;
    }
}
