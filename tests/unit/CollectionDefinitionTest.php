<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Filter;
use Utopia\Query\Schema\ColumnType;

final class CollectionDefinitionTest extends TestCase
{
    public function testCollectionDefinitionHasNoExternalId(): void
    {
        $definition = Database::collectionDefinition();

        $this->assertNotContains('externalId', $this->keys($definition));
        $this->assertSame(false, $definition->isSet('externalId'));
    }

    public function testCollectionDefinitionKeys(): void
    {
        $definition = Database::collectionDefinition();

        $this->assertSame(Database::METADATA, $definition->getId());
        $this->assertSame(Database::METADATA, $definition->getAttribute(Document::COLLECTION));
        $this->assertSame('collections', $definition->name());
        $this->assertSame(['name', 'attributes', 'indexes', 'documentSecurity'], $this->keys($definition));
    }

    public function testCollectionDefinitionAttributeTypes(): void
    {
        $byKey = [];
        foreach (Database::collectionDefinition()->attributes() as $attribute) {
            $byKey[$attribute->key] = $attribute;
        }

        $this->assertSame(ColumnType::String, $byKey['name']->type);
        $this->assertSame(256, $byKey['name']->size);
        $this->assertSame(true, $byKey['name']->required);

        $this->assertSame(ColumnType::String, $byKey['attributes']->type);
        $this->assertSame(1000000, $byKey['attributes']->size);
        $this->assertSame(true, \in_array(Filter::Json->value, $byKey['attributes']->filters, true));

        $this->assertSame(ColumnType::String, $byKey['indexes']->type);
        $this->assertSame(1000000, $byKey['indexes']->size);
        $this->assertSame(true, \in_array(Filter::Json->value, $byKey['indexes']->filters, true));

        $this->assertSame(ColumnType::Boolean, $byKey['documentSecurity']->type);
        $this->assertSame(true, $byKey['documentSecurity']->required);
    }

    public function testMetadataSchemaHasNoExternalId(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database->setDatabase('testing')->setNamespace('collections');
        $database->create();

        $this->assertNotContains('externalId', $this->keys($database->getCollection(Database::METADATA)));
    }

    /**
     * @return list<string>
     */
    private function keys(Collection $collection): array
    {
        return \array_map(static fn (Attribute $attribute): string => $attribute->key, $collection->attributes());
    }
}
