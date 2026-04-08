<?php

namespace Tests\Unit\ORM;

use PHPUnit\Framework\TestCase;
use Utopia\Database\ORM\Mapping\Column;
use Utopia\Database\ORM\Mapping\Entity;
use Utopia\Database\ORM\Mapping\Id;
use Utopia\Database\ORM\Mapping\PostPersist;
use Utopia\Database\ORM\Mapping\PostRemove;
use Utopia\Database\ORM\Mapping\PostUpdate;
use Utopia\Database\ORM\Mapping\PrePersist;
use Utopia\Database\ORM\Mapping\PreRemove;
use Utopia\Database\ORM\Mapping\PreUpdate;
use Utopia\Database\ORM\MetadataFactory;
use Utopia\Query\Schema\ColumnType;

#[Entity(collection: 'lifecycle_entities')]
class LifecycleEntity
{
    #[Id]
    public string $id = '';

    #[Column(type: ColumnType::String, size: 255)]
    public string $name = '';

    public array $callLog = [];

    #[PrePersist]
    public function onPrePersist(): void
    {
        $this->callLog[] = 'prePersist';
    }

    #[PostPersist]
    public function onPostPersist(): void
    {
        $this->callLog[] = 'postPersist';
    }

    #[PreUpdate]
    public function onPreUpdate(): void
    {
        $this->callLog[] = 'preUpdate';
    }

    #[PostUpdate]
    public function onPostUpdate(): void
    {
        $this->callLog[] = 'postUpdate';
    }

    #[PreRemove]
    public function onPreRemove(): void
    {
        $this->callLog[] = 'preRemove';
    }

    #[PostRemove]
    public function onPostRemove(): void
    {
        $this->callLog[] = 'postRemove';
    }
}

#[Entity(collection: 'multi_callback_entities')]
class MultiCallbackEntity
{
    #[Id]
    public string $id = '';

    #[Column(type: ColumnType::String, size: 255)]
    public string $name = '';

    public array $callLog = [];

    #[PrePersist]
    public function firstPrePersist(): void
    {
        $this->callLog[] = 'firstPrePersist';
    }

    #[PrePersist]
    public function secondPrePersist(): void
    {
        $this->callLog[] = 'secondPrePersist';
    }
}

#[Entity(collection: 'no_callback_entities')]
class NoCallbackEntity
{
    #[Id]
    public string $id = '';

    #[Column(type: ColumnType::String, size: 255)]
    public string $name = '';
}

class LifecycleCallbackTest extends TestCase
{
    protected MetadataFactory $factory;

    protected function setUp(): void
    {
        MetadataFactory::clearCache();
        $this->factory = new MetadataFactory();
    }

    public function testMetadataFactoryParsesPrePersistCallback(): void
    {
        $metadata = $this->factory->getMetadata(LifecycleEntity::class);

        $this->assertContains('onPrePersist', $metadata->prePersistCallbacks);
    }

    public function testMetadataFactoryParsesPostPersistCallback(): void
    {
        $metadata = $this->factory->getMetadata(LifecycleEntity::class);

        $this->assertContains('onPostPersist', $metadata->postPersistCallbacks);
    }

    public function testMetadataFactoryParsesPreUpdateCallback(): void
    {
        $metadata = $this->factory->getMetadata(LifecycleEntity::class);

        $this->assertContains('onPreUpdate', $metadata->preUpdateCallbacks);
    }

    public function testMetadataFactoryParsesPostUpdateCallback(): void
    {
        $metadata = $this->factory->getMetadata(LifecycleEntity::class);

        $this->assertContains('onPostUpdate', $metadata->postUpdateCallbacks);
    }

    public function testMetadataFactoryParsesPreRemoveCallback(): void
    {
        $metadata = $this->factory->getMetadata(LifecycleEntity::class);

        $this->assertContains('onPreRemove', $metadata->preRemoveCallbacks);
    }

    public function testMetadataFactoryParsesPostRemoveCallback(): void
    {
        $metadata = $this->factory->getMetadata(LifecycleEntity::class);

        $this->assertContains('onPostRemove', $metadata->postRemoveCallbacks);
    }

    public function testMetadataFactoryParsesMultipleCallbacksOfSameType(): void
    {
        $metadata = $this->factory->getMetadata(MultiCallbackEntity::class);

        $this->assertCount(2, $metadata->prePersistCallbacks);
        $this->assertContains('firstPrePersist', $metadata->prePersistCallbacks);
        $this->assertContains('secondPrePersist', $metadata->prePersistCallbacks);
    }

    public function testEntityWithoutCallbacksHasEmptyArrays(): void
    {
        $metadata = $this->factory->getMetadata(NoCallbackEntity::class);

        $this->assertEmpty($metadata->prePersistCallbacks);
        $this->assertEmpty($metadata->postPersistCallbacks);
        $this->assertEmpty($metadata->preUpdateCallbacks);
        $this->assertEmpty($metadata->postUpdateCallbacks);
        $this->assertEmpty($metadata->preRemoveCallbacks);
        $this->assertEmpty($metadata->postRemoveCallbacks);
    }

    public function testCallbackValuesAreStrings(): void
    {
        $metadata = $this->factory->getMetadata(LifecycleEntity::class);

        foreach ($metadata->prePersistCallbacks as $cb) {
            $this->assertIsString($cb);
        }

        foreach ($metadata->postPersistCallbacks as $cb) {
            $this->assertIsString($cb);
        }

        foreach ($metadata->preUpdateCallbacks as $cb) {
            $this->assertIsString($cb);
        }

        foreach ($metadata->postUpdateCallbacks as $cb) {
            $this->assertIsString($cb);
        }

        foreach ($metadata->preRemoveCallbacks as $cb) {
            $this->assertIsString($cb);
        }

        foreach ($metadata->postRemoveCallbacks as $cb) {
            $this->assertIsString($cb);
        }
    }

    public function testPrePersistCallbackCountIsExactlyOne(): void
    {
        $metadata = $this->factory->getMetadata(LifecycleEntity::class);

        $this->assertCount(1, $metadata->prePersistCallbacks);
    }

    public function testPostPersistCallbackCountIsExactlyOne(): void
    {
        $metadata = $this->factory->getMetadata(LifecycleEntity::class);

        $this->assertCount(1, $metadata->postPersistCallbacks);
    }

    public function testPreUpdateCallbackCountIsExactlyOne(): void
    {
        $metadata = $this->factory->getMetadata(LifecycleEntity::class);

        $this->assertCount(1, $metadata->preUpdateCallbacks);
    }

    public function testPostUpdateCallbackCountIsExactlyOne(): void
    {
        $metadata = $this->factory->getMetadata(LifecycleEntity::class);

        $this->assertCount(1, $metadata->postUpdateCallbacks);
    }

    public function testPreRemoveCallbackCountIsExactlyOne(): void
    {
        $metadata = $this->factory->getMetadata(LifecycleEntity::class);

        $this->assertCount(1, $metadata->preRemoveCallbacks);
    }

    public function testPostRemoveCallbackCountIsExactlyOne(): void
    {
        $metadata = $this->factory->getMetadata(LifecycleEntity::class);

        $this->assertCount(1, $metadata->postRemoveCallbacks);
    }
}
