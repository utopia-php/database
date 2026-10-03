<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use Tests\E2E\Adapter\Base;
use Tests\E2E\Adapter\MongoDBTest;
use Tests\E2E\Adapter\Schemaless\MongoDBTest as SchemalessMongoDBTest;
use Tests\E2E\Adapter\SharedTables\MongoDBTest as SharedTablesMongoDBTest;

final class MongoTestOverridesTest extends TestCase
{
    /**
     * @return array<string, array{class-string<Base>}>
     */
    public static function mongoTestClasses(): array
    {
        return [
            'MongoDB' => [MongoDBTest::class],
            'SharedTables/MongoDB' => [SharedTablesMongoDBTest::class],
            'Schemaless/MongoDB' => [SchemalessMongoDBTest::class],
        ];
    }

    /**
     * @param  class-string<Base>  $class
     */
    #[DataProvider('mongoTestClasses')]
    public function testOverridesUseTheNameOfTheTestTheyReplace(string $class): void
    {
        $inherited = [];
        foreach ((new ReflectionClass(Base::class))->getMethods() as $method) {
            $inherited[$this->comparable($method->getName())] = $method->getName();
        }

        $phantoms = [];
        foreach ((new ReflectionClass($class))->getMethods() as $method) {
            $name = $method->getName();
            if ($method->getDeclaringClass()->getName() !== $class || ! \str_starts_with(\strtolower($name), 'test')) {
                continue;
            }

            $replaced = $inherited[$this->comparable($name)] ?? null;
            if ($replaced !== null && \strtolower($replaced) !== \strtolower($name)) {
                $phantoms[$name] = $replaced;
            }
        }

        $this->assertSame(
            [],
            $phantoms,
            "{$class} declares tests that override nothing: PHP method names ignore case but not underscores, so the inherited tests still run",
        );
    }

    private function comparable(string $name): string
    {
        return \strtolower(\str_replace('_', '', $name));
    }
}
