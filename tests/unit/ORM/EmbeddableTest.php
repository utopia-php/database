<?php

namespace Tests\Unit\ORM;

use PHPUnit\Framework\TestCase;
use Utopia\Database\ORM\EmbeddableMapping;
use Utopia\Database\ORM\Mapping\Column;
use Utopia\Database\ORM\Mapping\Embedded;
use Utopia\Database\ORM\Mapping\Entity;
use Utopia\Database\ORM\Mapping\Id;
use Utopia\Database\ORM\MetadataFactory;
use Utopia\Query\Schema\ColumnType;

#[Entity(collection: 'embeddable_entities')]
class EmbeddableEntity
{
    #[Id]
    public string $id = '';

    #[Column(type: ColumnType::String, size: 255)]
    public string $name = '';

    #[Embedded(type: 'address')]
    public mixed $address = null;
}

#[Entity(collection: 'custom_prefix_entities')]
class CustomPrefixEmbeddableEntity
{
    #[Id]
    public string $id = '';

    #[Embedded(type: 'address', prefix: 'home_')]
    public mixed $homeAddress = null;
}

#[Entity(collection: 'multi_embeddable_entities')]
class MultiEmbeddableEntity
{
    #[Id]
    public string $id = '';

    #[Embedded(type: 'address')]
    public mixed $billing = null;

    #[Embedded(type: 'address', prefix: 'ship_')]
    public mixed $shipping = null;
}

#[Entity(collection: 'no_embeddable_entities')]
class NoEmbeddableEntity
{
    #[Id]
    public string $id = '';

    #[Column(type: ColumnType::String, size: 255)]
    public string $title = '';
}

class EmbeddableTest extends TestCase
{
    protected MetadataFactory $factory;

    protected function setUp(): void
    {
        MetadataFactory::clearCache();
        $this->factory = new MetadataFactory();
    }

    public function testMetadataFactoryParsesEmbeddedAttribute(): void
    {
        $metadata = $this->factory->getMetadata(EmbeddableEntity::class);

        $this->assertArrayHasKey('address', $metadata->embeddables);
    }

    public function testEmbeddableMappingHasCorrectPropertyName(): void
    {
        $metadata = $this->factory->getMetadata(EmbeddableEntity::class);
        $mapping = $metadata->embeddables['address'];

        $this->assertEquals('address', $mapping->propertyName);
    }

    public function testEmbeddableMappingHasCorrectTypeName(): void
    {
        $metadata = $this->factory->getMetadata(EmbeddableEntity::class);
        $mapping = $metadata->embeddables['address'];

        $this->assertEquals('address', $mapping->typeName);
    }

    public function testDefaultPrefixIsPropertyNameWithUnderscore(): void
    {
        $metadata = $this->factory->getMetadata(EmbeddableEntity::class);
        $mapping = $metadata->embeddables['address'];

        $this->assertEquals('address_', $mapping->prefix);
    }

    public function testCustomPrefixOverridesDefault(): void
    {
        $metadata = $this->factory->getMetadata(CustomPrefixEmbeddableEntity::class);
        $mapping = $metadata->embeddables['homeAddress'];

        $this->assertEquals('home_', $mapping->prefix);
    }

    public function testEntityWithoutEmbeddablesHasEmptyArray(): void
    {
        $metadata = $this->factory->getMetadata(NoEmbeddableEntity::class);

        $this->assertEmpty($metadata->embeddables);
    }

    public function testEmbeddableMappingIsInstanceOfEmbeddableMapping(): void
    {
        $metadata = $this->factory->getMetadata(EmbeddableEntity::class);
        $mapping = $metadata->embeddables['address'];

        $this->assertInstanceOf(EmbeddableMapping::class, $mapping);
    }

    public function testMultipleEmbeddablesAreParsed(): void
    {
        $metadata = $this->factory->getMetadata(MultiEmbeddableEntity::class);

        $this->assertCount(2, $metadata->embeddables);
        $this->assertArrayHasKey('billing', $metadata->embeddables);
        $this->assertArrayHasKey('shipping', $metadata->embeddables);
    }

    public function testMultipleEmbeddablesHaveDistinctPrefixes(): void
    {
        $metadata = $this->factory->getMetadata(MultiEmbeddableEntity::class);

        $this->assertEquals('billing_', $metadata->embeddables['billing']->prefix);
        $this->assertEquals('ship_', $metadata->embeddables['shipping']->prefix);
    }

    public function testEmbeddableMappingConstructorSetsReadonlyProperties(): void
    {
        $mapping = new EmbeddableMapping('myProp', 'myType', 'my_');

        $this->assertEquals('myProp', $mapping->propertyName);
        $this->assertEquals('myType', $mapping->typeName);
        $this->assertEquals('my_', $mapping->prefix);
    }
}
