<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;
use Utopia\Validator\Text;

final class StructureFormatTest extends TestCase
{
    private const string FORMAT = 'structureFormatProbe';

    /**
     * @var list<mixed>
     */
    private array $received = [];

    protected function setUp(): void
    {
        $this->received = [];
        Structure::addFormat(self::FORMAT, function (array $attribute): Text {
            $this->received[] = $attribute;
            $options = $attribute['formatOptions'] ?? [];
            $size = \is_array($options) ? ($options['maximum'] ?? 0) : 0;

            return new Text(\is_int($size) ? $size : 0);
        }, ColumnType::String);
    }

    protected function tearDown(): void
    {
        Structure::removeFormat(self::FORMAT);
    }

    public function testAFormatRegisteredForAnotherTypeIsRefused(): void
    {
        $this->assertSame(ColumnType::String->value, Structure::getFormat(self::FORMAT, ColumnType::String)['type']);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Format "'.self::FORMAT.'" not available for attribute type "integer"');

        Structure::getFormat(self::FORMAT, ColumnType::Integer);
    }

    /**
     * @return array<string, array{string, bool}>
     */
    public static function codes(): array
    {
        return [
            'within the format' => ['abc', true],
            'past the format' => ['abcdef', false],
        ];
    }

    #[DataProvider('codes')]
    public function testAFormatSeesAnAttributeDefinedAsAPlainArrayInItsStoredShape(string $code, bool $valid): void
    {
        $definition = $this->definition();
        $collection = $this->collection();
        $collection->setAttribute('attributes', [$definition]);

        $validator = new Structure($collection, ColumnType::Integer->value);

        $this->assertSame($valid, $validator->isValid($this->document($code)), $validator->getDescription());
        $this->assertSame([$this->storedShape($definition)], $this->received, 'the format callback receives the attribute in its stored shape');
    }

    #[DataProvider('codes')]
    public function testAFormatSeesAnAttributeDefinedAsADocumentInItsStoredShape(string $code, bool $valid): void
    {
        $definition = $this->definition();
        $collection = $this->collection();
        $collection->setAttribute('attributes', [new Document($definition)]);

        $validator = new Structure($collection, ColumnType::Integer->value);

        $this->assertSame($valid, $validator->isValid($this->document($code)), $validator->getDescription());
        $this->assertSame([$this->storedShape($definition)], $this->received);
    }

    /**
     * @return array<string, mixed>
     */
    private function definition(): array
    {
        return [
            Document::ID => 'code',
            'type' => ColumnType::String->value,
            'format' => self::FORMAT,
            'formatOptions' => ['maximum' => 4],
            'size' => 32,
            'required' => true,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ];
    }

    /**
     * @param  array<string, mixed>  $definition
     * @return array<string, mixed>
     */
    private function storedShape(array $definition): array
    {
        return Attribute::fromArray($definition)->toDocument()->getArrayCopy();
    }

    private function collection(): Document
    {
        return new Document([
            Document::ID => 'codes',
            Document::COLLECTION => Database::METADATA,
            'name' => 'codes',
            'attributes' => [],
            'indexes' => [],
        ]);
    }

    private function document(string $code): Document
    {
        return new Document([
            Document::COLLECTION => 'codes',
            'code' => $code,
            Document::CREATED_AT => '2026-09-30T00:00:00.000+00:00',
            Document::UPDATED_AT => '2026-09-30T00:00:00.000+00:00',
        ]);
    }
}
