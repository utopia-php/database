<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
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

        $keys = [];
        foreach ($attributes as $attribute) {
            $keys[] = $this->fields($attribute)['key'];
        }

        $this->assertSame(Database::METADATA, $definition[Document::ID]);
        $this->assertSame(Database::METADATA, $definition[Document::COLLECTION]);
        $this->assertSame('collections', $definition['name']);
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
        $this->assertIsArray($attributes);

        $keys = [];
        foreach ($attributes as $attribute) {
            $keys[] = $this->fields($attribute)['key'];
        }

        $this->assertNotContains('externalId', $keys);
    }

    /**
     * @param  array<string, mixed>  $definition
     * @return array{0: array<string, mixed>, 1: list<mixed>}
     */
    private function resolve(array $definition): array
    {
        $attributes = $definition['attributes'] ?? [];
        if (! \is_array($attributes)) {
            $attributes = [];
        }

        return [$definition, \array_values($attributes)];
    }

    /**
     * @return array{key: string, type: string, size: int, required: bool, filters: array<int, string>}
     */
    private function fields(mixed $attribute): array
    {
        if ($attribute instanceof Document) {
            $key = $attribute->getAttribute('key', $attribute->getId());
            $type = $attribute->getAttribute('type', '');
            $size = $attribute->getAttribute('size', 0);
            $required = $attribute->getAttribute('required', false);
            $filters = $attribute->getAttribute('filters', []);
        } elseif (\is_array($attribute)) {
            $key = $attribute['key'] ?? $attribute[Document::ID] ?? '';
            $type = $attribute['type'] ?? '';
            $size = $attribute['size'] ?? 0;
            $required = $attribute['required'] ?? false;
            $filters = $attribute['filters'] ?? [];
        } else {
            $this->fail('Attribute must be a Document or array');
        }

        if ($type instanceof \BackedEnum) {
            $type = (string) $type->value;
        }

        $this->assertIsString($key);
        $this->assertIsString($type);
        $this->assertIsInt($size);
        $this->assertIsBool($required);
        $this->assertIsArray($filters);

        $typedFilters = [];
        foreach ($filters as $filter) {
            $this->assertIsString($filter);
            $typedFilters[] = $filter;
        }

        return [
            'key' => $key,
            'type' => $type,
            'size' => $size,
            'required' => $required,
            'filters' => $typedFilters,
        ];
    }
}
