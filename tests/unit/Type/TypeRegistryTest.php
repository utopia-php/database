<?php

namespace Tests\Unit\Type;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Type\TypeRegistry;

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
        $filters = new \ReflectionProperty(Database::class, 'filters');
        $before = $filters->getValue();

        (new TypeRegistry())->register(new Reversed());

        $this->assertSame($before, $filters->getValue());
    }

    public function testDefaultFiltersNameEveryBuiltInFilter(): void
    {
        $filters = new \ReflectionProperty(Database::class, 'filters');
        $registered = new \ReflectionProperty(Database::class, 'defaultFiltersRegistered');
        $previousFilters = $filters->getValue();
        $previousRegistered = $registered->getValue();

        try {
            $filters->setValue(null, []);
            $registered->setValue(null, false);
            new Database(new Memory(), new Cache(new None()));

            $builtIn = $filters->getValue();
            $this->assertIsArray($builtIn);
            $names = \array_keys($builtIn);
            $expected = Database::DEFAULT_FILTERS;
            \sort($names);
            \sort($expected);

            $this->assertSame($expected, $names);
        } finally {
            $filters->setValue(null, $previousFilters);
            $registered->setValue(null, $previousRegistered);
        }
    }
}
