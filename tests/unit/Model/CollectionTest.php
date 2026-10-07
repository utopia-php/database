<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Exception\Index as IndexException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\SetType;
use Utopia\Query\OrderDirection;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

final class CollectionTest extends TestCase
{
    public function testConstructorIsPrivate(): void
    {
        $this->expectException(\Error::class);

        /** @phpstan-ignore new.privateConstructor */
        new Collection();
    }

    public function testCreateStoresEveryField(): void
    {
        $collection = Collection::create(
            id: 'books',
            name: 'Books',
            attributes: [Attribute::string('title', 128), Attribute::integer('pages')],
            indexes: [Index::key('by_title', ['title'], [], [OrderDirection::Asc])],
            permissions: [Permission::read(Role::any())],
            documentSecurity: false,
        );

        $this->assertSame('books', $collection->getId());
        $this->assertSame('Books', $collection->name());
        $this->assertSame(['title', 'pages'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $collection->attributes()));
        $this->assertSame([ColumnType::String, ColumnType::Integer], \array_map(static fn (Attribute $attribute): ColumnType => $attribute->type, $collection->attributes()));
        $this->assertSame(['by_title'], \array_map(static fn (Index $index): string => $index->key, $collection->indexes()));
        $this->assertSame([OrderDirection::Asc], $collection->indexes()[0]->orders);
        $this->assertSame(['read("any")'], $collection->declaredPermissions());
        $this->assertFalse($collection->documentSecurity());
        $this->assertFalse($collection->getAttribute('documentSecurity'));
    }

    public function testCreateDefaults(): void
    {
        $collection = Collection::create('books');

        $this->assertSame('books', $collection->name());
        $this->assertSame([], $collection->attributes());
        $this->assertSame([], $collection->indexes());
        $this->assertNull($collection->declaredPermissions());
        $this->assertTrue($collection->documentSecurity());
        $this->assertTrue($collection->getAttribute('documentSecurity'));
    }

    public function testCreateStoresAttributesAndIndexesAsDocuments(): void
    {
        $collection = Collection::create('books', attributes: [Attribute::string('title', 128)], indexes: [Index::fulltext('search', ['title'])]);

        $attributes = $collection->getAttribute('attributes');
        $indexes = $collection->getAttribute('indexes');

        $this->assertIsArray($attributes);
        $this->assertIsArray($indexes);
        $this->assertInstanceOf(Document::class, $attributes[0]);
        $this->assertInstanceOf(Document::class, $indexes[0]);
        $this->assertSame('title', $attributes[0]->getAttribute('key'));
        $this->assertSame('fulltext', $indexes[0]->getAttribute('type'));
    }

    public function testCreateWithNoPermissionsDeclaresAnEmptyList(): void
    {
        $this->assertSame([], Collection::create('books', permissions: [])->declaredPermissions());
    }

    public function testCreateKeepsMetadataInStorage(): void
    {
        $collection = Collection::create('books', metadata: ['category' => 'fiction', 'shelf' => 4]);

        $this->assertSame('fiction', $collection->getAttribute('category'));
        $this->assertSame(4, $collection->getAttribute('shelf'));
        $this->assertSame('fiction', $collection->toDocument()->getAttribute('category'));
    }

    public function testFromArrayKeepsExtrasAndNormalisesTheCoreKeys(): void
    {
        $collection = Collection::fromArray([
            '$id' => 'books',
            'attributes' => [
                ['$id' => 'title', 'key' => 'title', 'type' => 'string', 'size' => 128, 'required' => false, 'signed' => true, 'array' => false, 'filters' => []],
            ],
            'indexes' => [
                ['$id' => 'by_title', 'key' => 'by_title', 'type' => 'key', 'attributes' => ['title'], 'lengths' => [], 'orders' => ['desc']],
            ],
            'category' => 'fiction',
        ]);

        $this->assertSame('books', $collection->name());
        $this->assertTrue($collection->documentSecurity());
        $this->assertTrue($collection->getAttribute('documentSecurity'));
        $this->assertNull($collection->declaredPermissions());
        $this->assertSame('fiction', $collection->getAttribute('category'));
        $this->assertSame('title', $collection->attributes()[0]->key);
        $this->assertSame(ColumnType::String, $collection->attributes()[0]->type);
        $this->assertSame([OrderDirection::Desc], $collection->indexes()[0]->orders);
    }

    public function testFromArrayStoresModelsAndArraysAsDocuments(): void
    {
        $collection = Collection::fromArray([
            '$id' => 'books',
            'attributes' => [Attribute::string('title', 128), ['key' => 'pages', 'type' => 'integer']],
            'indexes' => [Index::key('by_title', ['title']), ['key' => 'by_pages', 'type' => 'key', 'attributes' => ['pages']]],
        ]);

        $this->assertContainsOnlyInstancesOf(Document::class, $collection->getArray('attributes'));
        $this->assertContainsOnlyInstancesOf(Document::class, $collection->getArray('indexes'));
        $this->assertSame(['title', 'pages'], \array_map(static fn (Attribute $attribute): string => $attribute->key, $collection->attributes()));
        $this->assertSame(['by_title', 'by_pages'], \array_map(static fn (Index $index): string => $index->key, $collection->indexes()));
    }

    public function testFromArrayReadsTheStoredDocumentSecurity(): void
    {
        $this->assertFalse(Collection::fromArray(['$id' => 'books', 'documentSecurity' => false])->documentSecurity());
    }

    public function testFromArrayDropsPermissionsThatAreNotAList(): void
    {
        $collection = Collection::fromArray(['$id' => 'books', '$permissions' => 'read("any")']);

        $this->assertNull($collection->declaredPermissions());
    }

    public function testFromArrayRejectsInvalidPermissionEntries(): void
    {
        $this->expectException(StructureException::class);

        Collection::fromArray(['$id' => 'books', '$permissions' => [1]]);
    }

    public function testFromArrayWithoutAnIdIsEmptyNamed(): void
    {
        $collection = Collection::fromArray([]);

        $this->assertSame('', $collection->getId());
        $this->assertSame('', $collection->name());
        $this->assertSame([], $collection->attributes());
    }

    public function testFromArrayKeepsEncodedListsUntilTheyAreDecoded(): void
    {
        $collection = Collection::fromArray(['$id' => 'books', 'attributes' => '[]', 'indexes' => '[]']);

        $this->assertSame('[]', $collection->getAttribute('attributes'));
        $this->assertSame([], $collection->attributes());
        $this->assertSame([], $collection->indexes());
    }

    public function testFromArrayRejectsAnAttributeThatIsNotADocument(): void
    {
        $this->expectException(StructureException::class);

        Collection::fromArray(['$id' => 'books', 'attributes' => ['title']]);
    }

    public function testFromArrayRejectsAnIndexThatIsNotADocument(): void
    {
        $this->expectException(IndexException::class);

        Collection::fromArray(['$id' => 'books', 'indexes' => [7]]);
    }

    public function testToDocumentIsAPlainIndependentCopy(): void
    {
        $collection = Collection::create('books', attributes: [Attribute::string('title', 128)], metadata: ['category' => 'fiction']);
        $document = $collection->toDocument();

        $this->assertSame(Document::class, $document::class);
        $this->assertSame($collection->getArrayCopy(), $document->getArrayCopy());

        $document->setAttribute('category', 'poetry');
        $document->getDocuments('attributes')[0]->setAttribute('key', 'heading');

        $this->assertSame('fiction', $collection->getAttribute('category'));
        $this->assertSame('title', $collection->attributes()[0]->key);
    }

    public function testNameFallsBackToTheId(): void
    {
        $this->assertSame('books', Collection::fromArray(['$id' => 'books', 'name' => ''])->name());
        $this->assertSame('books', Collection::fromArray(['$id' => 'books', 'name' => 5])->name());
        $this->assertSame('Books', Collection::fromArray(['$id' => 'books', 'name' => 'Books'])->name());
    }

    public function testDeclaredPermissionsFollowStorage(): void
    {
        $collection = Collection::create('books');
        $collection->setAttribute('$permissions', [Permission::read(Role::any())]);

        $this->assertSame(['read("any")'], $collection->declaredPermissions());

        $collection->removeAttribute('$permissions');

        $this->assertNull($collection->declaredPermissions());
    }

    public function testDocumentSecurityFollowsStorage(): void
    {
        $collection = Collection::create('books');
        $collection->setAttribute('documentSecurity', false);

        $this->assertFalse($collection->documentSecurity());
    }

    public function testAttributesAndIndexesAreHydratedOncePerInstance(): void
    {
        $collection = $this->collection();

        $attributes = $collection->attributes();
        $indexes = $collection->indexes();

        $this->assertSame($attributes, $collection->attributes());
        $this->assertSame($indexes, $collection->indexes());
    }

    public function testSetAttributeRefreshesTheMemo(): void
    {
        $collection = $this->collection();
        $collection->attributes();
        $collection->indexes();

        $collection->setAttribute('attributes', [Attribute::integer('pages')->toDocument()]);
        $collection->setAttribute('indexes', [Index::key('by_pages', ['pages'])->toDocument()]);

        $this->assertSame(['pages'], $this->attributeKeys($collection));
        $this->assertSame(['by_pages'], $this->indexKeys($collection));
    }

    public function testSetAttributeAppendRefreshesTheMemo(): void
    {
        $collection = $this->collection();
        $collection->attributes();
        $collection->indexes();

        $collection->setAttribute('attributes', Attribute::integer('pages')->toDocument(), SetType::Append);
        $collection->setAttribute('indexes', Index::key('by_pages', ['pages'])->toDocument(), SetType::Append);

        $this->assertSame(['title', 'pages'], $this->attributeKeys($collection));
        $this->assertSame(['by_title', 'by_pages'], $this->indexKeys($collection));
    }

    public function testSetAttributePrependRefreshesTheMemo(): void
    {
        $collection = $this->collection();
        $collection->attributes();
        $collection->indexes();

        $collection->setAttribute('attributes', Attribute::integer('pages')->toDocument(), SetType::Prepend);
        $collection->setAttribute('indexes', Index::key('by_pages', ['pages'])->toDocument(), SetType::Prepend);

        $this->assertSame(['pages', 'title'], $this->attributeKeys($collection));
        $this->assertSame(['by_pages', 'by_title'], $this->indexKeys($collection));
    }

    public function testSetAttributeWithTheSameMutatedListRefreshesTheMemo(): void
    {
        $collection = $this->collection();
        $collection->attributes();

        $stored = $collection->getDocuments('attributes');
        $stored[0]->setAttribute('key', 'heading');
        $collection->setAttribute('attributes', $collection->getAttribute('attributes'));

        $this->assertSame(['heading'], $this->attributeKeys($collection));
    }

    public function testSetAttributesRefreshesTheMemo(): void
    {
        $collection = $this->collection();
        $collection->attributes();
        $collection->indexes();

        $collection->setAttributes([
            'attributes' => [Attribute::integer('pages')->toDocument()],
            'indexes' => [],
        ]);

        $this->assertSame(['pages'], $this->attributeKeys($collection));
        $this->assertSame([], $this->indexKeys($collection));
    }

    public function testRemoveAttributeRefreshesTheMemo(): void
    {
        $collection = $this->collection();
        $collection->attributes();
        $collection->indexes();

        $collection->removeAttribute('attributes');
        $collection->removeAttribute('indexes');

        $this->assertSame([], $collection->attributes());
        $this->assertSame([], $collection->indexes());
    }

    public function testOffsetSetRefreshesTheMemo(): void
    {
        $collection = $this->collection();
        $collection->attributes();
        $collection->indexes();

        $collection['attributes'] = [Attribute::integer('pages')->toDocument()];
        $collection['indexes'] = [Index::unique('by_pages', ['pages'])->toDocument()];

        $this->assertSame(['pages'], $this->attributeKeys($collection));
        $this->assertSame(IndexType::Unique, $collection->indexes()[0]->type);
    }

    public function testOffsetUnsetRefreshesTheMemo(): void
    {
        $collection = $this->collection();
        $collection->attributes();
        $collection->indexes();

        unset($collection['attributes'], $collection['indexes']);

        $this->assertSame([], $collection->attributes());
        $this->assertSame([], $collection->indexes());
    }

    public function testExchangeArrayRefreshesTheMemo(): void
    {
        $collection = $this->collection();
        $collection->attributes();
        $collection->indexes();

        $collection->exchangeArray([
            '$id' => 'books',
            'attributes' => [Attribute::integer('pages')->toDocument()],
            'indexes' => [Index::key('by_pages', ['pages'])->toDocument()],
        ]);

        $this->assertSame(['pages'], $this->attributeKeys($collection));
        $this->assertSame(['by_pages'], $this->indexKeys($collection));
    }

    public function testAppendRefreshesTheMemo(): void
    {
        $collection = $this->collection();
        $attributes = $collection->attributes();
        $indexes = $collection->indexes();

        $collection->append('unkeyed');

        $this->assertNotSame($attributes[0], $collection->attributes()[0]);
        $this->assertNotSame($indexes[0], $collection->indexes()[0]);
        $this->assertSame(['title'], $this->attributeKeys($collection));
        $this->assertSame(['by_title'], $this->indexKeys($collection));
    }

    public function testIndirectWriteRefreshesTheMemo(): void
    {
        $collection = $this->collection();
        $collection->attributes();
        $collection->indexes();

        $attributes = &$collection['attributes'];
        $indexes = &$collection['indexes'];
        if (! \is_array($attributes) || ! \is_array($indexes)) {
            $this->fail('The collection stores its attributes and indexes as lists');
        }
        $attributes[] = Attribute::integer('pages')->toDocument();
        $indexes[] = Index::key('by_pages', ['pages'])->toDocument();

        $this->assertSame(['title', 'pages'], $this->attributeKeys($collection));
        $this->assertSame(['by_title', 'by_pages'], $this->indexKeys($collection));
    }

    public function testACloneDoesNotShareAStaleMemo(): void
    {
        $collection = $this->collection();
        $collection->attributes();

        $clone = clone $collection;
        $clone->setAttribute('attributes', Attribute::integer('pages')->toDocument(), SetType::Append);

        $this->assertSame(['title', 'pages'], $this->attributeKeys($clone));
        $this->assertSame(['title'], $this->attributeKeys($collection));
    }

    public function testStorageWritesToOtherKeysKeepTheListsCorrect(): void
    {
        $collection = $this->collection();
        $collection->attributes();

        $collection->setAttribute('name', 'Renamed');

        $this->assertSame(['title'], $this->attributeKeys($collection));
        $this->assertSame('Renamed', $collection->name());
    }

    private function collection(): Collection
    {
        return Collection::create(
            id: 'books',
            attributes: [Attribute::string('title', 128)],
            indexes: [Index::key('by_title', ['title'])],
        );
    }

    /**
     * @return list<string>
     */
    private function attributeKeys(Collection $collection): array
    {
        return \array_map(static fn (Attribute $attribute): string => $attribute->key, $collection->attributes());
    }

    /**
     * @return list<string>
     */
    private function indexKeys(Collection $collection): array
    {
        return \array_map(static fn (Index $index): string => $index->key, $collection->indexes());
    }
}
