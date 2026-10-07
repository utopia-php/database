<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Exception\Structure;
use Utopia\Database\Format;

final class FormatTest extends TestCase
{
    public function testHoldsNameAndOptions(): void
    {
        $format = new Format('intRange', ['min' => 1, 'max' => 10]);

        $this->assertSame('intRange', $format->name);
        $this->assertSame(['min' => 1, 'max' => 10], $format->options);
    }

    public function testOptionsDefaultToEmpty(): void
    {
        $this->assertSame([], (new Format('email'))->options);
    }

    public function testEmptyNameIsRejected(): void
    {
        $this->expectException(Structure::class);

        new Format('');
    }

    public function testPersistsAsFormatAndFormatOptions(): void
    {
        $stored = Attribute::integer('range', format: new Format('intRange', ['min' => 1]))->toDocument();

        $this->assertSame('intRange', $stored->getAttribute('format'));
        $this->assertSame(['min' => 1], $stored->getAttribute('formatOptions'));
    }

    public function testNoFormatPersistsAsNullWithEmptyOptions(): void
    {
        $stored = Attribute::string('name')->toDocument();

        $this->assertNull($stored->getAttribute('format'));
        $this->assertSame([], $stored->getAttribute('formatOptions'));
    }

    public function testOptionsWithoutAFormatNameAreDropped(): void
    {
        $attribute = Attribute::fromArray(['key' => 'name', 'type' => 'string', 'format' => null, 'formatOptions' => ['min' => 1]]);

        $this->assertNull($attribute->format);
        $this->assertSame([], $attribute->toDocument()->getAttribute('formatOptions'));
    }

    public function testNonArrayOptionsHydrateAsEmpty(): void
    {
        $attribute = Attribute::fromArray(['key' => 'name', 'type' => 'string', 'format' => 'email', 'formatOptions' => 'invalid']);

        $this->assertSame('email', $attribute->format?->name);
        $this->assertSame([], $attribute->format?->options);
    }
}
