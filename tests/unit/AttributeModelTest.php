<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Query\Schema\ColumnType;

class AttributeModelTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $attr = new Attribute();

        $this->assertSame('', $attr->key);
        $this->assertSame(ColumnType::String, $attr->type);
        $this->assertSame(0, $attr->size);
        $this->assertFalse($attr->required);
        $this->assertNull($attr->default);
        $this->assertTrue($attr->signed);
        $this->assertFalse($attr->array);
        $this->assertNull($attr->format);
        $this->assertSame([], $attr->formatOptions);
        $this->assertSame([], $attr->filters);
        $this->assertNull($attr->status);
        $this->assertNull($attr->options);
    }

    public function testConstructorWithAllValues(): void
    {
        $attr = new Attribute(
            key: 'score',
            type: ColumnType::Double,
            size: 0,
            required: true,
            default: 0.0,
            signed: true,
            array: false,
            format: 'number',
            formatOptions: ['min' => 0, 'max' => 100],
            filters: ['range'],
            status: 'available',
            options: ['precision' => 2],
        );

        $this->assertSame('score', $attr->key);
        $this->assertSame(ColumnType::Double, $attr->type);
        $this->assertSame(0, $attr->size);
        $this->assertTrue($attr->required);
        $this->assertSame(0.0, $attr->default);
        $this->assertTrue($attr->signed);
        $this->assertFalse($attr->array);
        $this->assertSame('number', $attr->format);
        $this->assertSame(['min' => 0, 'max' => 100], $attr->formatOptions);
        $this->assertSame(['range'], $attr->filters);
        $this->assertSame('available', $attr->status);
        $this->assertSame(['precision' => 2], $attr->options);
    }

    public function testToDocumentProducesCorrectStructure(): void
    {
        $attr = new Attribute(
            key: 'email',
            type: ColumnType::String,
            size: 256,
            required: true,
            default: null,
            signed: true,
            array: false,
            format: 'email',
            formatOptions: ['allowPlus' => true],
            filters: ['lowercase'],
        );

        $doc = $attr->toDocument();

        $this->assertInstanceOf(Document::class, $doc);
        $this->assertSame('email', $doc->getId());
        $this->assertSame('email', $doc->getAttribute('key'));
        $this->assertSame('string', $doc->getAttribute('type'));
        $this->assertSame(256, $doc->getAttribute('size'));
        $this->assertTrue($doc->getAttribute('required'));
        $this->assertNull($doc->getAttribute('default'));
        $this->assertTrue($doc->getAttribute('signed'));
        $this->assertFalse($doc->getAttribute('array'));
        $this->assertSame('email', $doc->getAttribute('format'));
        $this->assertSame(['allowPlus' => true], $doc->getAttribute('formatOptions'));
        $this->assertSame(['lowercase'], $doc->getAttribute('filters'));
    }

    public function testToDocumentIncludesStatusWhenSet(): void
    {
        $attr = new Attribute(key: 'name', type: ColumnType::String, status: 'processing');

        $doc = $attr->toDocument();
        $this->assertSame('processing', $doc->getAttribute('status'));
    }

    public function testToDocumentExcludesStatusWhenNull(): void
    {
        $attr = new Attribute(key: 'name', type: ColumnType::String);

        $doc = $attr->toDocument();
        $this->assertNull($doc->getAttribute('status'));
    }

    public function testToDocumentIncludesOptionsWhenSet(): void
    {
        $options = [
            'relatedCollection' => 'users',
            'relationType' => 'oneToMany',
            'twoWay' => true,
            'twoWayKey' => 'posts',
        ];
        $attr = new Attribute(key: 'author', type: ColumnType::Relationship, options: $options);

        $doc = $attr->toDocument();
        $this->assertSame($options, $doc->getAttribute('options'));
    }

    public function testToDocumentExcludesOptionsWhenNull(): void
    {
        $attr = new Attribute(key: 'name', type: ColumnType::String);

        $doc = $attr->toDocument();
        $this->assertNull($doc->getAttribute('options'));
    }

    public function testFromDocumentRoundtrip(): void
    {
        $original = new Attribute(
            key: 'tags',
            type: ColumnType::String,
            size: 64,
            required: false,
            default: null,
            signed: true,
            array: true,
            format: null,
            formatOptions: [],
            filters: ['json'],
        );

        $doc = $original->toDocument();
        $restored = Attribute::fromDocument($doc);

        $this->assertSame($original->key, $restored->key);
        $this->assertSame($original->type, $restored->type);
        $this->assertSame($original->size, $restored->size);
        $this->assertSame($original->required, $restored->required);
        $this->assertSame($original->default, $restored->default);
        $this->assertSame($original->signed, $restored->signed);
        $this->assertSame($original->array, $restored->array);
        $this->assertSame($original->format, $restored->format);
        $this->assertSame($original->formatOptions, $restored->formatOptions);
        $this->assertSame($original->filters, $restored->filters);
    }

    public function testFromDocumentWithMinimalDocument(): void
    {
        $doc = new Document(['$id' => 'name']);
        $attr = Attribute::fromDocument($doc);

        $this->assertSame('name', $attr->key);
        $this->assertSame(ColumnType::String, $attr->type);
        $this->assertSame(0, $attr->size);
        $this->assertFalse($attr->required);
        $this->assertTrue($attr->signed);
        $this->assertFalse($attr->array);
    }

    public function testFromDocumentUsesKeyOverId(): void
    {
        $doc = new Document(['$id' => 'id_val', 'key' => 'key_val', 'type' => 'string']);
        $attr = Attribute::fromDocument($doc);

        $this->assertSame('key_val', $attr->key);
    }

    public function testFromDocumentFallsBackToId(): void
    {
        $doc = new Document(['$id' => 'my_attr', 'type' => 'integer']);
        $attr = Attribute::fromDocument($doc);

        $this->assertSame('my_attr', $attr->key);
    }

    public function testFromArray(): void
    {
        $data = [
            'key' => 'amount',
            'type' => 'double',
            'size' => 0,
            'required' => true,
            'default' => 0.0,
            'signed' => true,
            'array' => false,
            'format' => null,
            'formatOptions' => [],
            'filters' => [],
        ];

        $attr = Attribute::fromArray($data);

        $this->assertSame('amount', $attr->key);
        $this->assertSame(ColumnType::Double, $attr->type);
        $this->assertTrue($attr->required);
        $this->assertSame(0.0, $attr->default);
    }

    public function testFromArrayWithIdFallback(): void
    {
        $data = ['$id' => 'my_field', 'type' => 'boolean'];
        $attr = Attribute::fromArray($data);

        $this->assertSame('my_field', $attr->key);
        $this->assertSame(ColumnType::Boolean, $attr->type);
    }

    public function testFromArrayDefaults(): void
    {
        $data = ['type' => 'integer'];
        $attr = Attribute::fromArray($data);

        $this->assertSame('', $attr->key);
        $this->assertSame(ColumnType::Integer, $attr->type);
        $this->assertSame(0, $attr->size);
        $this->assertFalse($attr->required);
        $this->assertNull($attr->default);
        $this->assertTrue($attr->signed);
        $this->assertFalse($attr->array);
        $this->assertNull($attr->format);
        $this->assertSame([], $attr->formatOptions);
        $this->assertSame([], $attr->filters);
    }

    public function testAllColumnTypeValues(): void
    {
        $typesToTest = [
            ColumnType::String,
            ColumnType::Varchar,
            ColumnType::Text,
            ColumnType::MediumText,
            ColumnType::LongText,
            ColumnType::Integer,
            ColumnType::Double,
            ColumnType::Boolean,
            ColumnType::Datetime,
            ColumnType::Relationship,
            ColumnType::Point,
            ColumnType::Linestring,
            ColumnType::Polygon,
            ColumnType::Vector,
            ColumnType::Object,
        ];

        foreach ($typesToTest as $type) {
            $attr = new Attribute(key: 'test_' . $type->value, type: $type);
            $doc = $attr->toDocument();
            $restored = Attribute::fromDocument($doc);

            $this->assertSame($type, $restored->type, "Roundtrip failed for type: {$type->value}");
        }
    }

    public function testWithFormatAndFormatOptions(): void
    {
        $attr = new Attribute(
            key: 'url',
            type: ColumnType::String,
            size: 2048,
            format: 'url',
            formatOptions: ['allowedSchemes' => ['http', 'https']],
        );

        $doc = $attr->toDocument();
        $this->assertSame('url', $doc->getAttribute('format'));
        $this->assertSame(['allowedSchemes' => ['http', 'https']], $doc->getAttribute('formatOptions'));

        $restored = Attribute::fromDocument($doc);
        $this->assertSame('url', $restored->format);
        $this->assertSame(['allowedSchemes' => ['http', 'https']], $restored->formatOptions);
    }

    public function testWithFilters(): void
    {
        $attr = new Attribute(
            key: 'content',
            type: ColumnType::String,
            size: 65535,
            filters: ['json', 'encrypt'],
        );

        $doc = $attr->toDocument();
        $this->assertSame(['json', 'encrypt'], $doc->getAttribute('filters'));

        $restored = Attribute::fromDocument($doc);
        $this->assertSame(['json', 'encrypt'], $restored->filters);
    }

    public function testWithRelationshipOptions(): void
    {
        $options = [
            'relatedCollection' => 'comments',
            'relationType' => 'oneToMany',
            'twoWay' => true,
            'twoWayKey' => 'post',
            'onDelete' => 'cascade',
            'side' => 'parent',
        ];

        $attr = new Attribute(
            key: 'comments',
            type: ColumnType::Relationship,
            options: $options,
        );

        $doc = $attr->toDocument();
        $restored = Attribute::fromDocument($doc);

        $this->assertSame($options, $restored->options);
    }

    public function testWithDefaultValueTypes(): void
    {
        $stringAttr = new Attribute(key: 's', type: ColumnType::String, size: 32, default: 'hello');
        $this->assertSame('hello', $stringAttr->default);

        $intAttr = new Attribute(key: 'i', type: ColumnType::Integer, default: 42);
        $this->assertSame(42, $intAttr->default);

        $boolAttr = new Attribute(key: 'b', type: ColumnType::Boolean, default: true);
        $this->assertTrue($boolAttr->default);

        $doubleAttr = new Attribute(key: 'd', type: ColumnType::Double, default: 3.14);
        $this->assertSame(3.14, $doubleAttr->default);

        $nullAttr = new Attribute(key: 'n', type: ColumnType::String, size: 32, default: null);
        $this->assertNull($nullAttr->default);
    }

    public function testFromArrayWithColumnTypeInstance(): void
    {
        $data = [
            'key' => 'test',
            'type' => ColumnType::Integer,
            'size' => 0,
        ];

        $attr = Attribute::fromArray($data);
        $this->assertSame(ColumnType::Integer, $attr->type);
    }

    public function testFromDocumentWithColumnTypeInstance(): void
    {
        $doc = new Document([
            '$id' => 'test',
            'key' => 'test',
            'type' => ColumnType::Boolean,
        ]);

        $attr = Attribute::fromDocument($doc);
        $this->assertSame(ColumnType::Boolean, $attr->type);
    }
}
