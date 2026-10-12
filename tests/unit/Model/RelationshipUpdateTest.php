<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipType;
use Utopia\Database\RelationshipUpdate;

final class RelationshipUpdateTest extends TestCase
{
    public function testDefaultsChangeNothing(): void
    {
        $update = new RelationshipUpdate();

        $this->assertNull($update->key);
        $this->assertNull($update->twoWayKey);
        $this->assertNull($update->twoWay);
        $this->assertNull($update->onDelete);
    }

    public function testEmptyUpdateKeepsEveryField(): void
    {
        $relationship = Relationship::oneToMany('comments', key: 'comments', twoWay: true, twoWayKey: 'post', onDelete: RelationshipDeleteAction::Cascade);

        $updated = $relationship->apply(new RelationshipUpdate());

        $this->assertSame('comments', $updated->relatedCollection);
        $this->assertSame(RelationshipType::OneToMany, $updated->type);
        $this->assertTrue($updated->twoWay);
        $this->assertSame('comments', $updated->key);
        $this->assertSame('post', $updated->twoWayKey);
        $this->assertSame(RelationshipDeleteAction::Cascade, $updated->onDelete);
    }

    public function testApplyChangesOnlyTheGivenFields(): void
    {
        $relationship = Relationship::manyToOne('users', key: 'author', twoWay: true, twoWayKey: 'posts');

        $updated = $relationship->apply(new RelationshipUpdate(key: 'writer', onDelete: RelationshipDeleteAction::SetNull));

        $this->assertSame('writer', $updated->key);
        $this->assertSame(RelationshipDeleteAction::SetNull, $updated->onDelete);
        $this->assertSame('posts', $updated->twoWayKey);
        $this->assertTrue($updated->twoWay);
        $this->assertSame('users', $updated->relatedCollection);
        $this->assertSame(RelationshipType::ManyToOne, $updated->type);
    }

    public function testApplyChangesEveryField(): void
    {
        $relationship = Relationship::oneToOne('profiles', key: 'profile', twoWay: true, twoWayKey: 'user', onDelete: RelationshipDeleteAction::Cascade);

        $updated = $relationship->apply(new RelationshipUpdate(
            key: 'avatar',
            twoWayKey: 'owner',
            twoWay: false,
            onDelete: RelationshipDeleteAction::Restrict,
        ));

        $this->assertSame('avatar', $updated->key);
        $this->assertSame('owner', $updated->twoWayKey);
        $this->assertFalse($updated->twoWay);
        $this->assertSame(RelationshipDeleteAction::Restrict, $updated->onDelete);
        $this->assertSame('profiles', $updated->relatedCollection);
        $this->assertSame(RelationshipType::OneToOne, $updated->type);
    }

    public function testFalseTwoWayIsAChange(): void
    {
        $updated = Relationship::manyToMany('tags', twoWay: true)->apply(new RelationshipUpdate(twoWay: false));

        $this->assertFalse($updated->twoWay);
    }

    public function testApplyResolvesAnUnresolvedKey(): void
    {
        $updated = Relationship::manyToOne('users')->apply(new RelationshipUpdate(key: 'author'));

        $this->assertSame('author', $updated->key);
        $this->assertNull($updated->twoWayKey);
    }

    public function testApplyLeavesTheOriginalUntouched(): void
    {
        $relationship = Relationship::oneToMany('comments', key: 'comments', twoWayKey: 'post');

        $relationship->apply(new RelationshipUpdate(key: 'replies', twoWayKey: 'thread', twoWay: true, onDelete: RelationshipDeleteAction::Cascade));

        $this->assertSame('comments', $relationship->key);
        $this->assertSame('post', $relationship->twoWayKey);
        $this->assertFalse($relationship->twoWay);
        $this->assertSame(RelationshipDeleteAction::Restrict, $relationship->onDelete);
    }
}
