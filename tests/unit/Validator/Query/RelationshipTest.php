<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\Validator\Query\Relationship as RelationshipValidator;
use Utopia\Query\Method;

class RelationshipTest extends TestCase
{
    /**
     * @return list<Attribute>
     */
    private function attributes(): array
    {
        return [
            Attribute::string('title'),
            Attribute::relationship('comments', Relationship::oneToMany('comments', twoWay: true, twoWayKey: 'post'), RelationshipSide::Parent),
            Attribute::relationship('tags', Relationship::manyToMany('tags', twoWay: true, twoWayKey: 'posts'), RelationshipSide::Parent),
            Attribute::relationship('profile', Relationship::oneToOne('profiles', twoWay: true, twoWayKey: 'post'), RelationshipSide::Parent),
            Attribute::relationship('author', Relationship::manyToOne('authors', twoWay: true, twoWayKey: 'posts'), RelationshipSide::Parent),
            Attribute::relationship('owner', Relationship::oneToMany('owners', twoWay: true, twoWayKey: 'items'), RelationshipSide::Child),
        ];
    }

    public function testAcceptsInnerQueriesOnPluralRelationship(): void
    {
        $validator = new RelationshipValidator($this->attributes());

        $this->assertTrue($validator->isValid(Query::relationship('comments', [
            Query::equal('approved', [true]),
            Query::orderDesc('$createdAt'),
            Query::limit(2),
            Query::offset(1),
            Query::cursorAfter(new Document(['$id' => 'comment1'])),
        ])));

        $this->assertTrue($validator->isValid(Query::relationship('tags', [
            Query::select(['name']),
            Query::limit(5),
        ])));
    }

    public function testRejectsWrongMethod(): void
    {
        $validator = new RelationshipValidator($this->attributes());

        $this->assertFalse($validator->isValid(Query::limit(1)));
        $this->assertSame('Invalid query method: limit', $validator->getDescription());
    }

    public function testRejectsNonRelationshipAttribute(): void
    {
        $validator = new RelationshipValidator($this->attributes());

        $this->assertFalse($validator->isValid(Query::relationship('title', [Query::limit(1)])));
        $this->assertSame(
            'Relationship queries can only be used on relationship attributes: title',
            $validator->getDescription(),
        );
    }

    public function testRejectsUnknownAttribute(): void
    {
        $validator = new RelationshipValidator($this->attributes());

        $this->assertFalse($validator->isValid(Query::relationship('doesNotExist', [Query::limit(1)])));
        $this->assertSame(
            'Relationship queries can only be used on relationship attributes: doesNotExist',
            $validator->getDescription(),
        );
    }

    public function testRejectsEmptyAttribute(): void
    {
        $validator = new RelationshipValidator($this->attributes());

        $this->assertFalse($validator->isValid(Query::relationship('', [Query::limit(1)])));
        $this->assertSame('Relationship queries require a relationship attribute', $validator->getDescription());
    }

    public function testRejectsNonQueryValues(): void
    {
        $validator = new RelationshipValidator($this->attributes());

        $this->assertFalse($validator->isValid(new Query(Method::Relationship, 'comments', ['approved'])));
        $this->assertSame('Relationship queries can only contain queries', $validator->getDescription());

        $this->assertFalse($validator->isValid(Query::relationship('comments', [])));
        $this->assertSame('Relationship queries can only contain queries', $validator->getDescription());
    }

    public function testRejectsRelationshipInRelationship(): void
    {
        $validator = new RelationshipValidator($this->attributes());

        $this->assertFalse($validator->isValid(Query::relationship('comments', [
            Query::relationship('author', [Query::limit(1)]),
        ])));
        $this->assertSame('Relationship queries cannot contain relationship queries', $validator->getDescription());
    }

    public function testRejectsMalformedInnerSelect(): void
    {
        $validator = new RelationshipValidator($this->attributes());

        $this->assertFalse($validator->isValid(Query::relationship('comments', [Query::select([])])));
        $this->assertSame('No attributes selected', $validator->getDescription());

        $this->assertFalse($validator->isValid(new Query(Method::Relationship, 'comments', [
            new Query(Method::Select, '', [123]),
        ])));
        $this->assertSame('Attribute selection must be a string, got int', $validator->getDescription());

        $this->assertFalse($validator->isValid(Query::relationship('comments', [Query::select(['text', 'text'])])));
        $this->assertSame('Duplicate attributes selected', $validator->getDescription());
    }

    public function testRejectsRelationshipInsideLogicalInnerQuery(): void
    {
        $validator = new RelationshipValidator($this->attributes());

        $this->assertFalse($validator->isValid(Query::relationship('comments', [
            Query::or([
                Query::relationship('author', [Query::limit(1)]),
                Query::equal('text', ['hi']),
            ]),
        ])));
        $this->assertSame('Relationship queries cannot contain relationship queries', $validator->getDescription());
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

    #[DataProvider('singularRelationships')]
    public function testRejectsPaginationOnSingularRelationship(string $attribute): void
    {
        $validator = new RelationshipValidator($this->attributes());

        foreach ([Query::limit(1), Query::offset(1), Query::cursorAfter(new Document(['$id' => 'x1']))] as $pagination) {
            $this->assertFalse($validator->isValid(Query::relationship($attribute, [$pagination])));
            $this->assertSame(
                'Relationship pagination is not supported on a singular relationship: '.$attribute,
                $validator->getDescription(),
            );
        }
    }

    #[DataProvider('singularRelationships')]
    public function testAcceptsFiltersOnSingularRelationship(string $attribute): void
    {
        $validator = new RelationshipValidator($this->attributes());

        $this->assertTrue($validator->isValid(Query::relationship($attribute, [
            Query::equal('name', ['Alice']),
            Query::select(['name']),
        ])));
    }

    public function testRejectsInvalidInnerLimit(): void
    {
        $validator = new RelationshipValidator($this->attributes());

        $this->assertFalse($validator->isValid(Query::relationship('comments', [Query::limit(0)])));
        $this->assertStringContainsString('Invalid limit', $validator->getDescription());

        $this->assertFalse($validator->isValid(Query::relationship('comments', [Query::limit(-1)])));
        $this->assertStringContainsString('Invalid limit', $validator->getDescription());
    }

    public function testRejectsInvalidInnerCursor(): void
    {
        $validator = new RelationshipValidator($this->attributes(), 4);

        $this->assertFalse($validator->isValid(Query::relationship('comments', [Query::cursorAfter(new Document(['$id' => 'waytoolongforfour']))])));
        $this->assertStringContainsString('Invalid cursor', $validator->getDescription());
    }

    public function testUnknownAttributeAcceptedWithoutAttributeSupport(): void
    {
        $validator = new RelationshipValidator($this->attributes(), 36, false);

        $this->assertTrue($validator->isValid(Query::relationship('doesNotExist', [Query::limit(1)])));
        $this->assertTrue($validator->isValid(Query::relationship('profile', [Query::limit(1)])));
    }
}
