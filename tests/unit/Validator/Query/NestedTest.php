<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Nested;

class NestedTest extends TestCase
{
    /**
     * @return array<Document>
     */
    private function attributes(): array
    {
        return [
            new Document([
                '$id' => 'title',
                'key' => 'title',
                'type' => Database::VAR_STRING,
                'array' => false,
            ]),
            new Document([
                '$id' => 'comments',
                'key' => 'comments',
                'type' => Database::VAR_RELATIONSHIP,
                'array' => false,
                'options' => [
                    'relationType' => Database::RELATION_ONE_TO_MANY,
                    'side' => Database::RELATION_SIDE_PARENT,
                    'relatedCollection' => 'comments',
                    'twoWay' => true,
                    'twoWayKey' => 'post',
                ],
            ]),
            new Document([
                '$id' => 'tags',
                'key' => 'tags',
                'type' => Database::VAR_RELATIONSHIP,
                'array' => false,
                'options' => [
                    'relationType' => Database::RELATION_MANY_TO_MANY,
                    'side' => Database::RELATION_SIDE_PARENT,
                    'relatedCollection' => 'tags',
                    'twoWay' => true,
                    'twoWayKey' => 'posts',
                ],
            ]),
            new Document([
                '$id' => 'profile',
                'key' => 'profile',
                'type' => Database::VAR_RELATIONSHIP,
                'array' => false,
                'options' => [
                    'relationType' => Database::RELATION_ONE_TO_ONE,
                    'side' => Database::RELATION_SIDE_PARENT,
                    'relatedCollection' => 'profiles',
                    'twoWay' => true,
                    'twoWayKey' => 'post',
                ],
            ]),
            new Document([
                '$id' => 'author',
                'key' => 'author',
                'type' => Database::VAR_RELATIONSHIP,
                'array' => false,
                'options' => [
                    'relationType' => Database::RELATION_MANY_TO_ONE,
                    'side' => Database::RELATION_SIDE_PARENT,
                    'relatedCollection' => 'authors',
                    'twoWay' => true,
                    'twoWayKey' => 'posts',
                ],
            ]),
            new Document([
                '$id' => 'owner',
                'key' => 'owner',
                'type' => Database::VAR_RELATIONSHIP,
                'array' => false,
                'options' => [
                    'relationType' => Database::RELATION_ONE_TO_MANY,
                    'side' => Database::RELATION_SIDE_CHILD,
                    'relatedCollection' => 'owners',
                    'twoWay' => true,
                    'twoWayKey' => 'items',
                ],
            ]),
        ];
    }

    public function testAcceptsInnerQueriesOnPluralRelationship(): void
    {
        $validator = new Nested($this->attributes());

        $this->assertTrue($validator->isValid(Query::nested('comments', [
            Query::equal('approved', [true]),
            Query::orderDesc('$createdAt'),
            Query::limit(2),
            Query::offset(1),
            Query::cursorAfter(new Document(['$id' => 'comment1'])),
        ])));

        $this->assertTrue($validator->isValid(Query::nested('tags', [
            Query::select(['name']),
            Query::limit(5),
        ])));
    }

    public function testRejectsWrongMethod(): void
    {
        $validator = new Nested($this->attributes());

        $this->assertFalse($validator->isValid(Query::limit(1)));
        $this->assertSame('Invalid query method: limit', $validator->getDescription());
    }

    public function testRejectsNonRelationshipAttribute(): void
    {
        $validator = new Nested($this->attributes());

        $this->assertFalse($validator->isValid(Query::nested('title', [Query::limit(1)])));
        $this->assertSame(
            'Nested queries can only be used on relationship attributes: title',
            $validator->getDescription()
        );
    }

    public function testRejectsUnknownAttribute(): void
    {
        $validator = new Nested($this->attributes());

        $this->assertFalse($validator->isValid(Query::nested('doesNotExist', [Query::limit(1)])));
        $this->assertSame(
            'Nested queries can only be used on relationship attributes: doesNotExist',
            $validator->getDescription()
        );
    }

    public function testRejectsEmptyAttribute(): void
    {
        $validator = new Nested($this->attributes());

        $this->assertFalse($validator->isValid(Query::nested('', [Query::limit(1)])));
        $this->assertSame('Nested queries require a relationship attribute', $validator->getDescription());
    }

    public function testRejectsNonQueryValues(): void
    {
        $validator = new Nested($this->attributes());

        $this->assertFalse($validator->isValid(new Query(Query::TYPE_NESTED, 'comments', ['approved'])));
        $this->assertSame('Nested queries can only contain queries', $validator->getDescription());

        $this->assertFalse($validator->isValid(Query::nested('comments', [])));
        $this->assertSame('Nested queries can only contain queries', $validator->getDescription());
    }

    public function testRejectsNestedInNested(): void
    {
        $validator = new Nested($this->attributes());

        $this->assertFalse($validator->isValid(Query::nested('comments', [
            Query::nested('author', [Query::limit(1)]),
        ])));
        $this->assertSame('Nested queries cannot be nested', $validator->getDescription());
    }

    /**
     * @return array<string, array{0: string}>
     */
    public static function singularRelationships(): array
    {
        return [
            'oneToOne' => ['profile'],
            'manyToOne parent' => ['author'],
            'oneToMany child' => ['owner'],
        ];
    }

    /**
     * @dataProvider singularRelationships
     */
    public function testRejectsPaginationOnSingularRelationship(string $attribute): void
    {
        $validator = new Nested($this->attributes());

        foreach ([Query::limit(1), Query::offset(1), Query::cursorAfter(new Document(['$id' => 'x1']))] as $pagination) {
            $this->assertFalse($validator->isValid(Query::nested($attribute, [$pagination])));
            $this->assertSame(
                'Nested pagination is not supported on a singular relationship: ' . $attribute,
                $validator->getDescription()
            );
        }
    }

    /**
     * @dataProvider singularRelationships
     */
    public function testAcceptsFiltersOnSingularRelationship(string $attribute): void
    {
        $validator = new Nested($this->attributes());

        $this->assertTrue($validator->isValid(Query::nested($attribute, [
            Query::equal('name', ['Alice']),
            Query::select(['name']),
        ])));
    }

    public function testRejectsInvalidInnerLimit(): void
    {
        $validator = new Nested($this->attributes());

        $this->assertFalse($validator->isValid(Query::nested('comments', [Query::limit(0)])));
        $this->assertStringContainsString('Invalid limit', $validator->getDescription());

        $this->assertFalse($validator->isValid(Query::nested('comments', [Query::limit(-1)])));
        $this->assertStringContainsString('Invalid limit', $validator->getDescription());
    }

    public function testRejectsInvalidInnerCursor(): void
    {
        $validator = new Nested($this->attributes(), 4);

        $this->assertFalse($validator->isValid(Query::nested('comments', [Query::cursorAfter(new Document(['$id' => 'waytoolongforfour']))])));
        $this->assertStringContainsString('Invalid cursor', $validator->getDescription());
    }

    public function testUnknownAttributeAcceptedWithoutAttributeSupport(): void
    {
        $validator = new Nested($this->attributes(), 36, false);

        $this->assertTrue($validator->isValid(Query::nested('doesNotExist', [Query::limit(1)])));
        $this->assertTrue($validator->isValid(Query::nested('profile', [Query::limit(1)])));
    }

    public function testGetMethodType(): void
    {
        $validator = new Nested($this->attributes());

        $this->assertSame(Nested::METHOD_TYPE_NESTED, $validator->getMethodType());
    }
}
