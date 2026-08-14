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

final class CollectionDefinitionTest extends TestCase
{
    public function testCollectionDefinitionHasNoExternalId(): void
    {
        $def = Database::collectionDefinition();
        [$definition, $attributes] = $this->resolve($def);

        $keys = [];
        foreach ($attributes as $attribute) {
            $keys[] = $this->fields($attribute)['key'];
        }

        $this->assertNotContains('externalId', $keys);
        $this->assertSame(false, \array_key_exists('externalId', $definition));
    }

    public function testCollectionDefinitionKeys(): void
    {
        $def = Database::collectionDefinition();
        [$definition, $attributes] = $this->resolve($def);

        $id = $definition[Document::ID] ?? ($def instanceof Collection ? $def->id : null);
        $collection = $definition[Document::COLLECTION] ?? ($def instanceof Collection ? $def->id : null);
        $name = $definition['name'] ?? ($def instanceof Collection ? $def->name : null);

        $keys = [];
        foreach ($attributes as $attribute) {
            $keys[] = $this->fields($attribute)['key'];
        }

        $this->assertSame(Database::METADATA, $id);
        $this->assertSame(Database::METADATA, $collection);
        $this->assertSame('collections', $name);
        $this->assertSame(['name', 'attributes', 'indexes', 'documentSecurity'], $keys);
    }

    public function testCollectionDefinitionAttributeTypes(): void
    {
        $def = Database::collectionDefinition();
        [, $attributes] = $this->resolve($def);

        $byKey = [];
        foreach ($attributes as $attribute) {
            $fields = $this->fields($attribute);
            $byKey[$fields['key']] = $fields;
        }

        $this->assertSame('string', $byKey['name']['type']);
        $this->assertSame(256, $byKey['name']['size']);
        $this->assertSame(true, $byKey['name']['required']);

        $this->assertSame('string', $byKey['attributes']['type']);
        $this->assertSame(1000000, $byKey['attributes']['size']);
        $this->assertSame(true, \in_array('json', $byKey['attributes']['filters'], true));

        $this->assertSame('string', $byKey['indexes']['type']);
        $this->assertSame(1000000, $byKey['indexes']['size']);
        $this->assertSame(true, \in_array('json', $byKey['indexes']['filters'], true));

        $this->assertSame('boolean', $byKey['documentSecurity']['type']);
        $this->assertSame(true, $byKey['documentSecurity']['required']);
    }

    public function testMetadataSchemaHasNoExternalId(): void
    {
        $db = new Database(new Memory(), new Cache(new None()));
        $db->setDatabase('testing')->setNamespace('collections');
        $db->create();

        $meta = $db->getCollection(Database::METADATA);
        $attributes = $meta->getAttribute('attributes', []);

        $keys = [];
        foreach ($attributes as $attribute) {
            $keys[] = $this->fields($attribute)['key'];
        }

        $this->assertNotContains('externalId', $keys);
    }

    /**
     * @param  Collection|array<string, mixed>  $definition
     * @return array{0: array<string, mixed>, 1: list<mixed>}
     */
    private function resolve(Collection|array $definition): array
    {
        if ($definition instanceof Collection) {
            $document = $definition->toDocument();

            return [$document->getArrayCopy(), $document->getAttribute('attributes')];
        }

        return [$definition, $definition['attributes'] ?? []];
    }

    /**
     * @return array{key: string, type: string, size: int, required: bool, filters: array<int, string>}
     */
    private function fields(mixed $attribute): array
    {
        if ($attribute instanceof Attribute) {
            $type = $attribute->type;

            return [
                'key' => $attribute->key,
                'type' => $type instanceof \BackedEnum ? $type->value : (string) $type,
                'size' => $attribute->size,
                'required' => $attribute->required,
                'filters' => $attribute->filters,
            ];
        }

        if ($attribute instanceof Document) {
            $type = $attribute->getAttribute('type');

            return [
                'key' => (string) $attribute->getAttribute('key', $attribute->getId()),
                'type' => $type instanceof \BackedEnum ? $type->value : (string) $type,
                'size' => (int) $attribute->getAttribute('size', 0),
                'required' => (bool) $attribute->getAttribute('required', false),
                'filters' => $attribute->getAttribute('filters', []),
            ];
        }

        $type = $attribute['type'] ?? '';

        return [
            'key' => (string) ($attribute['key'] ?? $attribute[Document::ID] ?? ''),
            'type' => $type instanceof \BackedEnum ? $type->value : (string) $type,
            'size' => (int) ($attribute['size'] ?? 0),
            'required' => (bool) ($attribute['required'] ?? false),
            'filters' => $attribute['filters'] ?? [],
        ];
    }
}
