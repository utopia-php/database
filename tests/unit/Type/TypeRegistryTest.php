<?php

namespace Tests\Unit\Type;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Type\CustomType;
use Utopia\Database\Type\TypeRegistry;
use Utopia\Query\Schema\ColumnType;

class TypeRegistryTest extends TestCase
{
    public function testRegisterAndGet(): void
    {
        $registry = new TypeRegistry();
        $type = new class () implements CustomType {
            public function name(): string
            {
                return 'money';
            }

            public function columnType(): ColumnType
            {
                return ColumnType::Integer;
            }

            public function columnSize(): int
            {
                return 0;
            }

            public function encode(mixed $value): mixed
            {
                return (int) ($value * 100);
            }

            public function decode(mixed $value): mixed
            {
                return $value / 100;
            }
        };

        $registry->register($type);

        $this->assertSame($type, $registry->get('money'));
        $this->assertNull($registry->get('nonexistent'));
    }

    public function testAll(): void
    {
        $registry = new TypeRegistry();
        $type = new class () implements CustomType {
            public function name(): string
            {
                return 'test_type';
            }

            public function columnType(): ColumnType
            {
                return ColumnType::String;
            }

            public function columnSize(): int
            {
                return 255;
            }

            public function encode(mixed $value): mixed
            {
                return $value;
            }

            public function decode(mixed $value): mixed
            {
                return $value;
            }
        };

        $registry->register($type);
        $all = $registry->all();

        $this->assertCount(1, $all);
        $this->assertArrayHasKey('test_type', $all);
    }
}
