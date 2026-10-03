<?php

namespace Tests\Unit\Collections;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;

final class MetadataModelTest extends TestCase
{
    public function testMetadataCollectionReadsAreIndependentCopies(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $expected = Collection::fromArray((new Document(Database::collectionDefinition()))->getArrayCopy())->getArrayCopy();

        $first = $database->getCollection(Database::METADATA);
        $this->assertSame($expected, $first->getArrayCopy());

        /** @var list<Attribute> $attributes */
        $attributes = $first->getAttribute('attributes');
        $attributes[0]->key = 'renamed';
        $attributes[0]->setAttribute('size', 1);
        $first->setAttribute('name', 'changed');

        $second = $database->getCollection(Database::METADATA);
        $this->assertSame($expected, $second->getArrayCopy());
        $this->assertNotSame($first, $second);
        /** @var list<Attribute> $secondAttributes */
        $secondAttributes = $second->getAttribute('attributes');
        $this->assertNotSame($attributes[0], $secondAttributes[0]);
    }

    public function testMetadataDefinitionReadsAreIndependentCopies(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $expected = (new Document(Database::collectionDefinition()))->getArrayCopy();

        $first = $database->getDocument(Database::METADATA, Database::METADATA);
        $this->assertSame($expected, $first->getArrayCopy());

        /** @var list<Attribute> $attributes */
        $attributes = $first->getAttribute('attributes');
        $attributes[0]->key = 'renamed';
        $first->setAttribute('name', 'changed');

        $second = $database->getDocument(Database::METADATA, Database::METADATA);
        $this->assertSame($expected, $second->getArrayCopy());
        /** @var list<Attribute> $secondAttributes */
        $secondAttributes = $second->getAttribute('attributes');
        $this->assertNotSame($attributes[0], $secondAttributes[0]);
    }
}
