<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\IntegerWidth;

final class IntegerWidthTest extends TestCase
{
    public function testSizes(): void
    {
        $this->assertNull(IntegerWidth::Bits32->size());
        $this->assertSame(8, IntegerWidth::Bits64->size());
    }

    /**
     * @return array<string, array{?int, IntegerWidth}>
     */
    public static function storedSizes(): array
    {
        return [
            'none' => [null, IntegerWidth::Bits32],
            'zero' => [0, IntegerWidth::Bits32],
            'four' => [4, IntegerWidth::Bits32],
            'seven' => [7, IntegerWidth::Bits32],
            'eight' => [8, IntegerWidth::Bits64],
            'sixteen' => [16, IntegerWidth::Bits64],
        ];
    }

    #[DataProvider('storedSizes')]
    public function testEnginesOnlyDistinguishSizesOfEightAndAbove(?int $size, IntegerWidth $width): void
    {
        $this->assertSame($width, IntegerWidth::fromSize($size));
        $this->assertSame($width, Attribute::fromArray(['key' => 'count', 'type' => 'integer', 'size' => $size])->width());
    }

    public function testEachWidthRoundTripsThroughItsSize(): void
    {
        foreach (IntegerWidth::cases() as $width) {
            $this->assertSame($width, IntegerWidth::fromSize($width->size()));
        }
    }

    public function testIntegerFactoryWidthSurvivesStorage(): void
    {
        foreach (IntegerWidth::cases() as $width) {
            $stored = Attribute::integer('count', width: $width)->toDocument();

            $this->assertSame($width->size() ?? 0, $stored->getAttribute('size'));
            $this->assertSame($width, Attribute::fromDocument($stored)->width());
        }
    }
}
