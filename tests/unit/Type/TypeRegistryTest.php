<?php

namespace Tests\Unit\Type;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\FilterRegistry;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Type\TypeRegistry;
use Utopia\Query\Schema\ColumnType;

final class TypeRegistryTest extends TestCase
{
    public function testRegisterAndGet(): void
    {
        $registry = new TypeRegistry();
        $type = new Reversed();

        $registry->register($type);

        $this->assertSame($type, $registry->get('reversed'));
        $this->assertNull($registry->get('nonexistent'));
    }

    public function testAll(): void
    {
        $registry = new TypeRegistry();
        $reversed = new Reversed();
        $rot13 = new Rot13();

        $registry->register($reversed);
        $registry->register($rot13);

        $this->assertSame(['reversed' => $reversed, 'rot13' => $rot13], $registry->all());
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function builtInFilters(): iterable
    {
        foreach (['json', 'datetime', 'point', 'linestring', 'polygon', 'vector', 'object'] as $name) {
            yield $name => [$name];
        }
    }

    #[DataProvider('builtInFilters')]
    public function testBuiltInFilterNamesAreRejected(string $name): void
    {
        $registry = new TypeRegistry();

        try {
            $registry->register(new Reversed($name));
            $this->fail("registering a type named \"{$name}\" must be rejected");
        } catch (DuplicateException $exception) {
            $this->assertStringContainsString("\"{$name}\"", $exception->getMessage());
        }

        $this->assertSame([], $registry->all());
    }

    public function testRegisteringLeavesTheGlobalFiltersUntouched(): void
    {
        (new TypeRegistry())->register(new Reversed());

        $notes = new Document([
            '$id' => 'notes',
            'attributes' => [
                new Document([
                    '$id' => 'body',
                    'type' => ColumnType::String->value,
                    'array' => false,
                    'filters' => ['reversed'],
                ]),
            ],
        ]);

        $this->expectException(NotFoundException::class);
        (new Database(new Memory(), new Cache(new None())))->decode($notes, new Document(['body' => 'olleh']));
    }

    public function testDefaultFiltersNameEveryBuiltInFilter(): void
    {
        $previousFilters = FilterRegistry::filters();
        $previousRegistered = FilterRegistry::defaultsRegistered();

        try {
            FilterRegistry::clear();
            new Database(new Memory(), new Cache(new None()));

            $names = \array_keys(FilterRegistry::filters());
            $expected = Database::DEFAULT_FILTERS;
            \sort($names);
            \sort($expected);

            $this->assertSame($expected, $names);
        } finally {
            FilterRegistry::restore($previousFilters, $previousRegistered);
        }
    }
}
