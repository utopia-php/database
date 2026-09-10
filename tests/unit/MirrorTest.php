<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Mirror;

class MirrorTest extends TestCase
{
    public function testSetDatabaseUpdatesMirrorAndChildren(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->setDatabase('utopiaTests');

        $this->assertSame('utopiaTests', $mirror->getDatabase());
        $this->assertSame('utopiaTests', $source->getDatabase());
        $this->assertSame('utopiaTests', $destination->getDatabase());
    }

    public function testSetNamespaceUpdatesMirrorAndChildren(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->setNamespace('myapp');

        $this->assertSame('myapp', $mirror->getNamespace());
        $this->assertSame('myapp', $source->getNamespace());
        $this->assertSame('myapp', $destination->getNamespace());
    }

    public function testSetTenantUpdatesMirrorAndChildren(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->setTenant(7);

        $this->assertSame(7, $mirror->getTenant());
        $this->assertSame(7, $source->getTenant());
        $this->assertSame(7, $destination->getTenant());
    }

    public function testSetSharedTablesUpdatesMirrorAndChildren(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->setSharedTables(true);

        $this->assertTrue($mirror->getSharedTables());
        $this->assertTrue($source->getSharedTables());
        $this->assertTrue($destination->getSharedTables());
    }

    public function testCreateCreatesMetadataOnDestination(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror
            ->setDatabase('utopiaTests')
            ->setNamespace('myapp')
            ->create();

        $this->assertTrue($source->exists('utopiaTests'));
        $this->assertTrue($source->exists('utopiaTests', Database::METADATA));
        $this->assertTrue($destination->exists('utopiaTests'));
        $this->assertTrue($destination->exists('utopiaTests', Database::METADATA));
    }

    public function testListCollectionsHidesSourceOnlyUpgrades(): void
    {
        [$mirror, $source] = $this->pair();

        $mirror
            ->setDatabase('utopiaTests')
            ->setNamespace('myapp')
            ->create();

        $mirror->createCollection(new Collection(id: 'actors', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));

        $listed = $mirror->listCollections();
        $ids = \array_map(static fn ($collection): string => $collection->getId(), $listed);

        $this->assertSame(['actors'], $ids);
        $this->assertFalse($source->getCollection('upgrades')->isEmpty());
    }

    public function testSkipValidationRestoresSourceAndDestination(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $this->assertTrue($mirror->isValidationEnabled());
        $this->assertTrue($source->isValidationEnabled());
        $this->assertTrue($destination->isValidationEnabled());

        $mirror->skipValidation(function () use ($mirror, $source, $destination) {
            $this->assertFalse($mirror->isValidationEnabled());
            $this->assertFalse($source->isValidationEnabled());
            $this->assertFalse($destination->isValidationEnabled());
        });

        $this->assertTrue($mirror->isValidationEnabled());
        $this->assertTrue($source->isValidationEnabled());
        $this->assertTrue($destination->isValidationEnabled());
    }

    public function testDisableValidationDelegatesToSourceAndDestination(): void
    {
        [$mirror, $source, $destination] = $this->pair();

        $mirror->disableValidation();

        $this->assertFalse($mirror->isValidationEnabled());
        $this->assertFalse($source->isValidationEnabled());
        $this->assertFalse($destination->isValidationEnabled());

        $mirror->enableValidation();

        $this->assertTrue($mirror->isValidationEnabled());
        $this->assertTrue($source->isValidationEnabled());
        $this->assertTrue($destination->isValidationEnabled());
    }

    public function testCreateThrowsWhenDestinationCreateFails(): void
    {
        $source = new Database(new Memory(), new Cache(new None()));
        $destination = new Database(new class () extends Memory {
            public function create(string $name): bool
            {
                throw new RuntimeException('destination create failed');
            }
        }, new Cache(new None()));
        $mirror = new Mirror($source, $destination);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('destination create failed');

        $mirror->setDatabase('utopiaTests')->setNamespace('myapp')->create();
    }

    /**
     * @return array{0: Mirror, 1: Database, 2: Database}
     */
    private function pair(): array
    {
        $source = new Database(new Memory(), new Cache(new None()));
        $destination = new Database(new Memory(), new Cache(new None()));

        return [new Mirror($source, $destination), $source, $destination];
    }
}
