<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Index;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

final class CollectionFromDocumentTest extends TestCase
{
    public function testACollectionIsReturnedAsItIs(): void
    {
        $collection = Collection::create('books', attributes: [Attribute::string('title', 64)]);

        $this->assertSame($collection, Collection::fromDocument($collection));
    }

    public function testTheHydratedAttributesOfACollectionAreKept(): void
    {
        $collection = Collection::create('books', attributes: [Attribute::string('title', 64)]);
        $attributes = $collection->attributes();

        $this->assertSame($attributes[0], Collection::fromDocument($collection)->attributes()[0]);
    }

    public function testAPlainDefinitionIsHydrated(): void
    {
        $definition = Collection::create(
            'books',
            attributes: [Attribute::string('title', 64), Attribute::integer('pages', required: true)],
            indexes: [Index::key('by_pages', ['pages'])],
        )->toDocument();

        $collection = Collection::fromDocument($definition);

        $this->assertNotSame($definition, $collection);
        $this->assertSame('books', $collection->getId());
        $this->assertSame(['title', 'pages'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $collection->attributes()));
        $this->assertSame(ColumnType::Integer, $collection->attributes()[1]->type);
        $this->assertTrue($collection->attributes()[1]->required);
        $this->assertSame('by_pages', $collection->indexes()[0]->key);
        $this->assertSame(IndexType::Key, $collection->indexes()[0]->type);
    }

    public function testAPlainDefinitionWithoutAttributesHasNone(): void
    {
        $collection = Collection::fromDocument(new Document([Document::ID => 'empty']));

        $this->assertSame([], $collection->attributes());
        $this->assertSame([], $collection->indexes());
    }

    public function testAnAttributeOfAnUnknownTypeIsRefused(): void
    {
        $collection = Collection::fromDocument(new Document([
            Document::ID => 'books',
            'attributes' => [new Document([Document::ID => 'title', 'type' => 'mystery'])],
        ]));

        $this->expectException(StructureException::class);

        $collection->attributes();
    }
}
