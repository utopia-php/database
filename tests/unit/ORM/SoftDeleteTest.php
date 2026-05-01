<?php

namespace Tests\Unit\ORM;

use PHPUnit\Framework\TestCase;
use Utopia\Database\ORM\EntityMapper;
use Utopia\Database\ORM\EntityState;
use Utopia\Database\ORM\IdentityMap;
use Utopia\Database\ORM\Mapping\Column;
use Utopia\Database\ORM\Mapping\Entity;
use Utopia\Database\ORM\Mapping\Id;
use Utopia\Database\ORM\Mapping\SoftDelete;
use Utopia\Database\ORM\MetadataFactory;
use Utopia\Database\ORM\UnitOfWork;
use Utopia\Query\Schema\ColumnType;

#[Entity(collection: 'soft_items')]
#[SoftDelete]
class SoftDeleteEntity
{
    #[Id]
    public string $id = '';

    #[Column(type: ColumnType::String, size: 255)]
    public string $name = '';

    public ?string $deletedAt = null;
}

#[Entity(collection: 'custom_soft_items')]
#[SoftDelete(column: 'removedAt')]
class CustomSoftDeleteEntity
{
    #[Id]
    public string $id = '';

    #[Column(type: ColumnType::String, size: 255)]
    public string $name = '';

    public ?string $removedAt = null;
}

#[Entity(collection: 'hard_items')]
class HardDeleteEntity
{
    #[Id]
    public string $id = '';

    #[Column(type: ColumnType::String, size: 255)]
    public string $name = '';
}

class SoftDeleteTest extends TestCase
{
    protected MetadataFactory $metadataFactory;

    protected IdentityMap $identityMap;

    protected UnitOfWork $uow;

    protected function setUp(): void
    {
        MetadataFactory::clearCache();
        $this->metadataFactory = new MetadataFactory();
        $this->identityMap = new IdentityMap();
        $mapper = new EntityMapper($this->metadataFactory);
        $this->uow = new UnitOfWork($this->identityMap, $this->metadataFactory, $mapper);
    }

    public function testMetadataFactoryParsesSoftDeleteAttribute(): void
    {
        $metadata = $this->metadataFactory->getMetadata(SoftDeleteEntity::class);

        $this->assertEquals('deletedAt', $metadata->softDeleteColumn);
    }

    public function testMetadataFactoryParsesSoftDeleteWithCustomColumn(): void
    {
        $metadata = $this->metadataFactory->getMetadata(CustomSoftDeleteEntity::class);

        $this->assertEquals('removedAt', $metadata->softDeleteColumn);
    }

    public function testEntityWithoutSoftDeleteHasNullColumn(): void
    {
        $metadata = $this->metadataFactory->getMetadata(HardDeleteEntity::class);

        $this->assertNull($metadata->softDeleteColumn);
    }

    public function testRemoveSetsDeletedAtOnSoftDeletableEntity(): void
    {
        $entity = new SoftDeleteEntity();
        $entity->id = 'soft-1';
        $entity->name = 'Soft';

        $metadata = $this->metadataFactory->getMetadata(SoftDeleteEntity::class);
        $this->identityMap->put('soft_items', 'soft-1', $entity);
        $this->uow->registerManaged($entity, $metadata);

        $this->assertNull($entity->deletedAt);

        $this->uow->remove($entity);

        $this->assertNotNull($entity->deletedAt);
        $this->assertEquals(EntityState::Managed, $this->uow->getState($entity));
    }

    public function testRemoveSchedulesDeletionOnNonSoftDeletableEntity(): void
    {
        $entity = new HardDeleteEntity();
        $entity->id = 'hard-1';
        $entity->name = 'Hard';

        $metadata = $this->metadataFactory->getMetadata(HardDeleteEntity::class);
        $this->identityMap->put('hard_items', 'hard-1', $entity);
        $this->uow->registerManaged($entity, $metadata);

        $this->uow->remove($entity);

        $this->assertEquals(EntityState::Removed, $this->uow->getState($entity));
    }

    public function testForceRemoveAlwaysSchedulesRealDeletion(): void
    {
        $entity = new SoftDeleteEntity();
        $entity->id = 'force-1';
        $entity->name = 'Force';

        $metadata = $this->metadataFactory->getMetadata(SoftDeleteEntity::class);
        $this->identityMap->put('soft_items', 'force-1', $entity);
        $this->uow->registerManaged($entity, $metadata);

        $this->uow->forceRemove($entity);

        $this->assertEquals(EntityState::Removed, $this->uow->getState($entity));
    }

    public function testForceRemoveOnNonSoftDeletableEntitySchedulesDeletion(): void
    {
        $entity = new HardDeleteEntity();
        $entity->id = 'force-hard-1';
        $entity->name = 'ForceHard';

        $metadata = $this->metadataFactory->getMetadata(HardDeleteEntity::class);
        $this->identityMap->put('hard_items', 'force-hard-1', $entity);
        $this->uow->registerManaged($entity, $metadata);

        $this->uow->forceRemove($entity);

        $this->assertEquals(EntityState::Removed, $this->uow->getState($entity));
    }

    public function testRestoreClearsDeletedAt(): void
    {
        $entity = new SoftDeleteEntity();
        $entity->id = 'restore-1';
        $entity->name = 'Restore';
        $entity->deletedAt = '2024-01-01 00:00:00';

        $this->uow->restore($entity);

        $this->assertNull($entity->deletedAt);
    }

    public function testRestoreIsNoOpWithoutSoftDelete(): void
    {
        $entity = new HardDeleteEntity();
        $entity->id = 'restore-hard-1';
        $entity->name = 'RestoreHard';

        $this->uow->restore($entity);

        $this->assertNull($this->uow->getState($entity));
    }

    public function testSoftDeleteDoesNotScheduleDeletion(): void
    {
        $entity = new SoftDeleteEntity();
        $entity->id = 'no-schedule-1';
        $entity->name = 'NoSchedule';

        $metadata = $this->metadataFactory->getMetadata(SoftDeleteEntity::class);
        $this->identityMap->put('soft_items', 'no-schedule-1', $entity);
        $this->uow->registerManaged($entity, $metadata);

        $this->uow->remove($entity);

        $this->assertNotEquals(EntityState::Removed, $this->uow->getState($entity));
    }

    public function testRestoreWithCustomColumnClearsValue(): void
    {
        $entity = new CustomSoftDeleteEntity();
        $entity->id = 'restore-custom-1';
        $entity->name = 'RestoreCustom';
        $entity->removedAt = '2024-06-15 12:00:00';

        $this->uow->restore($entity);

        $this->assertNull($entity->removedAt);
    }
}
