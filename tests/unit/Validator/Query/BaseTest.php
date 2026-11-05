<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Validator\Query\Base;

final class BaseTest extends TestCase
{
    #[DataProvider('joinColumns')]
    public function testAllowsOnlyAPlainIdentifierAsAJoinColumn(string $column, bool $allowed): void
    {
        $validator = new class () extends Base {
            public function allows(string $column): bool
            {
                return $this->isAllowedJoinColumn($column);
            }

            #[\Override]
            public function getMethodType(): string
            {
                return self::METHOD_TYPE_FILTER;
            }
        };

        $this->assertSame($allowed, $validator->allows($column));
    }

    /**
     * @return iterable<string, array{string, bool}>
     */
    public static function joinColumns(): iterable
    {
        yield 'an identifier' => ['title', true];
        yield 'an internal attribute' => ['$id', true];
        yield 'an empty name' => ['', false];
        yield 'a dotted path' => ['a.b', false];
        yield 'a trailing newline' => ["title\n", false];
    }
}
