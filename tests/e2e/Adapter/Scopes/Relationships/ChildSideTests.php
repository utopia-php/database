<?php

namespace Tests\E2E\Adapter\Scopes\Relationships;

use PHPUnit\Framework\Attributes\DataProvider;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Database\RelationshipUpdate;

trait ChildSideTests
{
    /**
     * @return array<string, array{RelationshipType}>
     */
    public static function childSideRelationshipTypes(): array
    {
        return [
            'oneToMany' => [RelationshipType::OneToMany],
            'manyToOne' => [RelationshipType::ManyToOne],
            'manyToMany' => [RelationshipType::ManyToMany],
        ];
    }

    #[DataProvider('childSideRelationshipTypes')]
    public function testChildSideUpdateRenamesBothKeys(RelationshipType $type): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, $children] = $this->createChildSideRelationship($database, $type);

        $updated = $database->updateRelationship($children, 'back', new RelationshipUpdate(key: 'owner', twoWayKey: 'items'));

        $this->assertSame($parents, $updated->relatedCollection);
        $this->assertSame($type, $updated->type);
        $this->assertSame('owner', $updated->key);
        $this->assertSame('items', $updated->twoWayKey);

        $this->assertNull($this->childSideAttribute($database, $children, 'back'));
        $this->assertNull($this->childSideAttribute($database, $parents, 'related'));

        $child = $this->childSideAttribute($database, $children, 'owner');
        $this->assertSame(RelationshipSide::Child, $child?->side);
        $this->assertSame('items', $child?->relationship?->twoWayKey);

        $parent = $this->childSideAttribute($database, $parents, 'items');
        $this->assertSame(RelationshipSide::Parent, $parent?->side);
        $this->assertSame('owner', $parent?->relationship?->twoWayKey);

        $this->assertSame(['c1'], $this->childSideIds($database->getDocument($parents, 'p1')->getAttribute('items')));
        $this->assertSame(['p1'], $this->childSideIds($database->getDocument($children, 'c1')->getAttribute('owner')));
    }

    #[DataProvider('childSideRelationshipTypes')]
    public function testChildSideUpdateChangesTheDeleteActionOfBothSides(RelationshipType $type): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, $children] = $this->createChildSideRelationship($database, $type);

        $updated = $database->updateRelationship($children, 'back', new RelationshipUpdate(onDelete: RelationshipDeleteAction::Cascade));

        $this->assertSame(RelationshipDeleteAction::Cascade, $updated->onDelete);
        $this->assertSame('back', $updated->key);
        $this->assertSame('related', $updated->twoWayKey);
        $this->assertSame(RelationshipDeleteAction::Cascade, $this->childSideAttribute($database, $children, 'back')?->relationship?->onDelete);
        $this->assertSame(RelationshipDeleteAction::Cascade, $this->childSideAttribute($database, $parents, 'related')?->relationship?->onDelete);

        if ($type === RelationshipType::ManyToOne) {
            $database->deleteDocument($children, 'c1');
            $this->assertTrue($database->getDocument($parents, 'p1')->isEmpty());
        } else {
            $database->deleteDocument($parents, 'p1');
            $this->assertTrue($database->getDocument($children, 'c1')->isEmpty());
        }
    }

    #[DataProvider('childSideRelationshipTypes')]
    public function testChildSideDeleteRemovesBothSides(RelationshipType $type): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->hasFeature(Feature\Relationships::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$parents, $children] = $this->createChildSideRelationship($database, $type);
        $junction = '_'.$database->getCollection($parents)->getSequence().'_'.$database->getCollection($children)->getSequence();

        $database->deleteRelationship($children, 'back');

        $this->assertNull($this->childSideAttribute($database, $children, 'back'));
        $this->assertNull($this->childSideAttribute($database, $parents, 'related'));

        $parent = $database->getDocument($parents, 'p1');
        $child = $database->getDocument($children, 'c1');
        $this->assertFalse($parent->isEmpty());
        $this->assertFalse($child->isEmpty());
        $this->assertNull($parent->getAttribute('related'));
        $this->assertNull($child->getAttribute('back'));

        if ($type === RelationshipType::ManyToMany) {
            $this->assertNull($database->findCollection($junction));
        }

        $database->createRelationship($parents, $this->childSideDefinition($type, $children));

        $this->assertSame([], $this->childSideIds($database->getDocument($parents, 'p1')->getAttribute('related')));
        $this->assertSame([], $this->childSideIds($database->getDocument($children, 'c1')->getAttribute('back')));
    }

    /**
     * Two collections related two-way from the first, the parent, under the key "related", with the child side
     * stored as "back", and one document on each side linked to the other.
     *
     * @return array{string, string}
     */
    private function createChildSideRelationship(Database $database, RelationshipType $type): array
    {
        $parents = 'child_side_parents_'.ID::unique();
        $children = 'child_side_children_'.ID::unique();
        $permissions = [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];

        $database->createCollection(Collection::create($parents, attributes: [Attribute::string('name', 64)], permissions: $permissions, documentSecurity: false));
        $database->createCollection(Collection::create($children, attributes: [Attribute::string('name', 64)], permissions: $permissions, documentSecurity: false));
        $database->createRelationship($parents, $this->childSideDefinition($type, $children));

        $database->createDocument($children, new Document(['$id' => 'c1', 'name' => 'child']));
        $database->createDocument($parents, new Document([
            '$id' => 'p1',
            'name' => 'parent',
            'related' => $type === RelationshipType::ManyToOne ? 'c1' : ['c1'],
        ]));

        return [$parents, $children];
    }

    private function childSideDefinition(RelationshipType $type, string $children): Relationship
    {
        return match ($type) {
            RelationshipType::OneToOne => Relationship::oneToOne($children, key: 'related', twoWay: true, twoWayKey: 'back'),
            RelationshipType::OneToMany => Relationship::oneToMany($children, key: 'related', twoWay: true, twoWayKey: 'back'),
            RelationshipType::ManyToOne => Relationship::manyToOne($children, key: 'related', twoWay: true, twoWayKey: 'back'),
            RelationshipType::ManyToMany => Relationship::manyToMany($children, key: 'related', twoWay: true, twoWayKey: 'back'),
        };
    }

    private function childSideAttribute(Database $database, string $collection, string $key): ?Attribute
    {
        foreach ($database->getCollection($collection)->attributes() as $attribute) {
            if ($attribute->key === $key) {
                return $attribute;
            }
        }

        return null;
    }

    /**
     * @return list<string>
     */
    private function childSideIds(mixed $value): array
    {
        $ids = [];
        foreach (\is_array($value) ? $value : [$value] as $item) {
            if ($item instanceof Document && ! $item->isEmpty()) {
                $ids[] = $item->getId();
            } elseif (\is_string($item) && $item !== '') {
                $ids[] = $item;
            }
        }
        \sort($ids);

        return $ids;
    }
}
