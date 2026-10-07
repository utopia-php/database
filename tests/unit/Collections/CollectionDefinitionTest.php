<?php

namespace Tests\Unit\Collections;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Query\Schema\ColumnType;

final class CollectionDefinitionTest extends TestCase
{
    public function testTheMetadataCollectionIsDefined(): void
    {
        $definition = Database::collectionDefinition();

        $this->assertSame(Database::METADATA, $definition->getId());
        $this->assertSame('collections', $definition->name());
        $this->assertSame(Database::METADATA, $definition->getCollection());
        $this->assertFalse($definition->documentSecurity());
        $this->assertSame([], $definition->indexes());
    }

    public function testTheMetadataAttributesKeepTheirStoredShape(): void
    {
        $attributes = [];
        foreach (Database::collectionDefinition()->attributes() as $attribute) {
            $attributes[$attribute->key] = [$attribute->type, $attribute->size, $attribute->required, $attribute->filters];
        }

        $this->assertSame([
            'name' => [ColumnType::String, 256, true, []],
            'attributes' => [ColumnType::String, 1_000_000, false, ['json']],
            'indexes' => [ColumnType::String, 1_000_000, false, ['json']],
            'documentSecurity' => [ColumnType::Boolean, null, true, []],
        ], $attributes);
    }

    public function testEveryCallReturnsAnIndependentCopy(): void
    {
        $first = Database::collectionDefinition();
        $first->setAttribute('name', 'changed');
        $first->setAttribute('attributes', []);

        $second = Database::collectionDefinition();

        $this->assertSame('collections', $second->name());
        $this->assertCount(4, $second->attributes());
    }

    public function testGetCollectionReturnsTheDefinitionForTheMetadataCollection(): void
    {
        $database = $this->database(sharedTables: false);

        $this->assertSame(
            Database::collectionDefinition()->getArrayCopy(),
            $database->getCollection(Database::METADATA)->getArrayCopy(),
        );
    }

    public function testInternalAttributesLeaveOutTheTenantWithoutSharedTables(): void
    {
        $keys = \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $this->database(sharedTables: false)->internalAttributes(),
        );

        $this->assertSame([
            Document::ID,
            Document::SEQUENCE,
            Document::COLLECTION,
            Document::CREATED_AT,
            Document::UPDATED_AT,
            Document::PERMISSIONS,
        ], $keys);
    }

    public function testInternalAttributesIncludeTheTenantUnderSharedTables(): void
    {
        $keys = \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $this->database(sharedTables: true)->internalAttributes(),
        );

        $this->assertSame([
            Document::ID,
            Document::SEQUENCE,
            Document::COLLECTION,
            Document::TENANT,
            Document::CREATED_AT,
            Document::UPDATED_AT,
            Document::PERMISSIONS,
        ], $keys);
    }

    public function testInternalAttributesFollowTheSharedTablesSetting(): void
    {
        $database = $this->database(sharedTables: false);
        $database->setSharedTables(true);

        $this->assertContains(Document::TENANT, $this->internalKeys($database));

        $database->setSharedTables(false);

        $this->assertNotContains(Document::TENANT, $this->internalKeys($database));
    }

    /**
     * @return list<string>
     */
    private function internalKeys(Database $database): array
    {
        return \array_map(static fn (Attribute $attribute): string => $attribute->key, $database->internalAttributes());
    }

    public function testInternalAttributesKeepTheirStoredShape(): void
    {
        $stored = [];
        foreach ($this->database(sharedTables: true)->internalAttributes() as $attribute) {
            $document = $attribute->toDocument();
            $stored[$attribute->key] = [
                $document->getAttribute('type'),
                $document->getAttribute('size'),
                $document->getAttribute('required'),
                $document->getAttribute('default'),
                $document->getAttribute('signed'),
                $document->getAttribute('array'),
                $document->getAttribute('filters'),
            ];
        }

        $this->assertSame([
            Document::ID => ['string', Database::LENGTH_KEY, true, null, true, false, []],
            Document::SEQUENCE => ['id', 0, true, null, true, false, []],
            Document::COLLECTION => ['string', Database::LENGTH_KEY, true, null, true, false, []],
            Document::TENANT => ['id', 0, false, null, true, false, []],
            Document::CREATED_AT => ['datetime', 0, false, null, false, false, ['datetime']],
            Document::UPDATED_AT => ['datetime', 0, false, null, false, false, ['datetime']],
            Document::PERMISSIONS => ['string', 1_000_000, false, [], true, false, ['json']],
        ], $stored);
    }

    private function database(bool $sharedTables): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setDatabase('definition')
            ->setNamespace('definition_'.\uniqid())
            ->setSharedTables($sharedTables);

        return $database;
    }
}
