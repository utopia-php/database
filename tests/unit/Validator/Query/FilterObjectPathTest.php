<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Query\Schema\ColumnType;

final class FilterObjectPathTest extends TestCase
{
    private Filter $validator;

    #[\Override]
    protected function setUp(): void
    {
        $this->validator = new Filter(
            attributes: [
                Attribute::object(key: 'meta'),
                Attribute::string(key: 'secret', size: 64),
            ],
            idAttributeType: ColumnType::Integer->value,
        );
    }

    /**
     * @return array<string, array{Query}>
     */
    public static function acceptedPaths(): array
    {
        return [
            'one key' => [Query::equal('meta.a', ['x'])],
            'nested keys' => [Query::equal('meta.user-info.home_city2', ['x'])],
            'starts with' => [Query::startsWith('meta.user.email', 'alice@')],
            'is null' => [Query::isNull('meta.a')],
            'inside or' => [Query::or([Query::equal('meta.a', ['x']), Query::equal('meta.b', ['y'])])],
            'base attribute' => [Query::equal('meta', [['a' => 'x']])],
        ];
    }

    #[DataProvider('acceptedPaths')]
    public function testAcceptsPathsOfPlainKeys(Query $query): void
    {
        $this->assertTrue($this->validator->isValid($query), $this->validator->getDescription());
    }

    /**
     * @return array<string, array{Query}>
     */
    public static function refusedPaths(): array
    {
        return [
            'quote in the last key' => [Query::equal("meta.a' IN ('x') OR secret='s2' OR 'x", ['x'])],
            'operator in the last key' => [Query::equal("meta.a'||(select 1)||'", ['x'])],
            'comment in the last key' => [Query::equal("meta.a' OR 1=1 --", ['x'])],
            'quote in a middle key' => [Query::equal("meta.a'b.c", ['x'])],
            'space in a key' => [Query::equal('meta.first name', ['x'])],
            'empty key' => [Query::equal('meta..a', ['x'])],
            'trailing dot' => [Query::equal('meta.', ['x'])],
            'trailing newline' => [Query::equal("meta.a\n", ['x'])],
            'less than' => [Query::lessThan("meta.a'", '5')],
            'starts with' => [Query::startsWith("meta.a'", 'x')],
            'is null' => [Query::isNull("meta.a'")],
            'inside or' => [Query::or([Query::equal('meta.a', ['x']), Query::equal("meta.b'", ['y'])])],
            'inside and' => [Query::and([Query::equal('meta.a', ['x']), Query::isNotNull("meta.b'")])],
        ];
    }

    #[DataProvider('refusedPaths')]
    public function testRefusesPathsWithKeysOutsideTheAllowedChars(Query $query): void
    {
        $this->assertFalse($this->validator->isValid($query));
    }

    public function testRefusesUnsafePathsWithoutDefinedAttributes(): void
    {
        $validator = new Filter(
            attributes: [Attribute::object(key: 'meta')],
            idAttributeType: ColumnType::Integer->value,
            supportForAttributes: false,
        );

        $this->assertTrue($validator->isValid(Query::equal('meta.a', ['x'])));
        $this->assertFalse($validator->isValid(Query::equal("meta.a' OR 'x", ['x'])));
    }
}
