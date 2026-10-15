<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Validator\Spatial;
use Utopia\Query\Schema\ColumnType;

final class SpatialTypeTest extends TestCase
{
    /**
     * @return array<string, array{string}>
     */
    public static function nonSpatialTypes(): array
    {
        return [
            'string' => [ColumnType::String->value],
            'vector' => [ColumnType::Vector->value],
            'empty' => [''],
            'unknown name' => ['circle'],
        ];
    }

    #[DataProvider('nonSpatialTypes')]
    public function testAnArrayForAValidatorOfANonSpatialTypeIsAnUnknownSpatialType(string $type): void
    {
        $validator = new Spatial($type);

        $this->assertFalse($validator->isValid([1.0, 2.0]));
        $this->assertStringEndsWith('Unknown spatial type: '.$type, $validator->getDescription());
        $this->assertTrue($validator->isValid(null), 'null stays valid whatever the type');
    }

    public function testAnArrayForAPointValidatorIsCheckedAsAPoint(): void
    {
        $validator = new Spatial(ColumnType::Point->value);

        $this->assertTrue($validator->isValid([1.0, 2.0]), $validator->getDescription());
        $this->assertStringNotContainsString('Unknown spatial type', $validator->getDescription());
    }
}
