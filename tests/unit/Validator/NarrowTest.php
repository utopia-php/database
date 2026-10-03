<?php

namespace Tests\Unit\Validator;

use DateTime;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Queries\Bounds;
use Utopia\Database\Validator\Queries\Documents;
use Utopia\Database\Validator\Queries\Narrow;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

final class NarrowTest extends TestCase
{
    private const int MAX_VALUES = 3;

    /**
     * @return array<string, array{list<Query>}>
     */
    public static function narrowLists(): array
    {
        $lists = [
            'no queries' => [],
            'equal string' => [Query::equal('title', ['Dune'])],
            'equal string with a number' => [Query::equal('title', [1])],
            'equal too many values' => [Query::equal('title', ['a', 'b', 'c', 'd'])],
            'equal no values' => [Query::equal('title', [])],
            'equal integer' => [Query::equal('count', [5])],
            'equal integer with a string' => [Query::equal('count', ['five'])],
            'integer over 32 bits' => [Query::greaterThan('count', 2147483648)],
            'big integer column over 32 bits' => [Query::greaterThan('big', 2147483648)],
            'unsigned below zero' => [Query::lessThan('unsigned', -1)],
            'bigint signed' => [Query::equal('huge', ['9223372036854775807'])],
            'bigint unsigned' => [Query::equal('hugeUnsigned', ['18446744073709551615'])],
            'float' => [Query::lessThanEqual('price', 9.5)],
            'float with a string' => [Query::lessThanEqual('price', 'cheap')],
            'boolean' => [Query::equal('active', [true])],
            'boolean with a string' => [Query::equal('active', ['yes'])],
            'datetime' => [Query::greaterThanEqual('published', '2020-01-01T00:00:00.000+00:00')],
            'datetime out of range' => [Query::greaterThanEqual('published', '10000-01-01T00:00:00.000+00:00')],
            'datetime not a date' => [Query::greaterThanEqual('published', 'yesterday-ish')],
            'between' => [Query::between('price', 1, 2)],
            'between one value' => [new Query(Method::Between, 'price', [1])],
            'not between' => [Query::notBetween('count', 1, 9)],
            'single value with two' => [new Query(Method::LessThan, 'count', [1, 2])],
            'starts with' => [Query::startsWith('title', 'Du')],
            'not starts with' => [Query::notStartsWith('title', 'Du')],
            'ends with' => [Query::endsWith('title', 'ne')],
            'not ends with' => [Query::notEndsWith('title', 'ne')],
            'regex' => [Query::regex('title', '^D')],
            'contains on an array' => [Query::contains('tags', ['sci-fi'])],
            'contains on a string' => [Query::contains('title', ['un'])],
            'contains on an integer' => [Query::contains('count', [1])],
            'contains no values' => [Query::contains('tags', [])],
            'contains any' => [Query::containsAny('tags', ['a', 'b'])],
            'contains all' => [Query::containsAll('tags', ['a', 'b'])],
            'not contains' => [Query::notContains('tags', ['a'])],
            'equal on an array' => [Query::equal('tags', ['a'])],
            'is null' => [Query::isNull('title')],
            'is not null on an array' => [Query::isNotNull('tags')],
            'not equal' => [Query::notEqual('title', 'Dune')],
            'encrypted' => [Query::equal('secret', ['x'])],
            'missing attribute' => [Query::equal('missing', ['x'])],
            'empty attribute' => [Query::equal('', ['x'])],
            'virtual one to one child' => [Query::equal('author', ['x'])],
            'virtual one to many parent' => [Query::equal('books', ['x'])],
            'virtual many to one child' => [Query::equal('editor', ['x'])],
            'many to many' => [Query::equal('readers', ['x'])],
            'many to one parent' => [Query::equal('owner', ['x'])],
            'object containment' => [Query::equal('meta', [['level' => 1]])],
            'object mixed list' => [Query::equal('meta', [['a' => [1, 'b' => [2]]]])],
            'point with equal' => [Query::equal('location', [[1, 2]])],
            'point not an array' => [Query::equal('location', ['here'])],
            'vector with equal' => [Query::equal('embedding', [[1, 2, 3]])],
            'id' => [Query::equal('$id', ['dune'])],
            'sequence' => [Query::equal('$sequence', ['12'])],
            'sequence not a number' => [Query::equal('$sequence', ['twelve'])],
            'created at' => [Query::greaterThan('$createdAt', '2020-01-01T00:00:00.000+00:00')],
            'updated at not a date' => [Query::lessThan('$updatedAt', 'soon')],
            'permissions are not an attribute' => [Query::equal('$permissions', ['read("any")'])],
            'duplicated key, last definition wins' => [Query::equal('dup', ['x'])],
            'duplicated key with a number' => [Query::equal('dup', [5])],
            'limit' => [Query::limit(25)],
            'limit zero' => [Query::limit(0)],
            'offset' => [Query::offset(10)],
            'offset below zero' => [Query::offset(-1)],
            'cursor after' => [Query::cursorAfter(new Document([Document::ID => 'dune']))],
            'cursor before an id' => [Query::cursorBefore(new Document([Document::ID => 'dune']))],
            'cursor too long' => [Query::cursorAfter(new Document([Document::ID => \str_repeat('x', 40)]))],
            'order asc' => [Query::orderAsc('title')],
            'order desc on a missing attribute' => [Query::orderDesc('missing')],
            'order asc on an internal attribute' => [Query::orderAsc('$sequence')],
            'order on an empty attribute' => [Query::orderAsc('')],
            'order random' => [Query::orderRandom()],
            'a page' => [Query::equal('title', ['Dune']), Query::greaterThan('count', 1), Query::orderDesc('$createdAt'), Query::limit(10), Query::offset(20)],
            'the first refusal is the reported one' => [Query::equal('title', ['Dune']), Query::equal('missing', ['x']), Query::equal('count', ['five'])],
            'a refusal after an order' => [Query::orderAsc('title'), Query::limit(5), Query::equal('active', ['yes'])],
            'cursor page' => [Query::cursorAfter(new Document([Document::ID => 'dune'])), Query::orderAsc('price'), Query::isNotNull('title')],
        ];

        $named = [];
        foreach ($lists as $name => $queries) {
            $named[$name] = [$queries];
        }

        return $named;
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('narrowLists')]
    public function testANarrowListIsJudgedAsTheDocumentsValidatorJudgesIt(array $queries): void
    {
        $this->assertTrue(Narrow::accepts($queries));

        foreach ([true, false] as $supportForAttributes) {
            foreach ([true, false] as $orderRandom) {
                $documents = $this->documents($supportForAttributes, $orderRandom);
                $narrow = $this->narrow($queries, $supportForAttributes, $orderRandom);
                $this->assertNotNull($narrow);

                $valid = $documents->isValid($queries);

                $context = ($supportForAttributes ? 'defined attributes' : 'schemaless').($orderRandom ? ', random order' : '');
                $this->assertSame($valid, $narrow->isValid($queries), $context.': '.$documents->getDescription());
                if (! $valid) {
                    $this->assertSame($documents->getDescription(), $narrow->getDescription(), $context);
                }
            }
        }
    }

    /**
     * @return array<string, array{list<mixed>}>
     */
    public static function otherLists(): array
    {
        return [
            'dotted filter' => [[Query::equal('author.name', ['x'])]],
            'dotted order' => [[Query::orderAsc('meta.level')]],
            'search' => [[Query::search('title', 'dune')]],
            'not search' => [[Query::notSearch('title', 'dune')]],
            'or' => [[Query::or([Query::equal('title', ['a']), Query::equal('title', ['b'])])]],
            'and' => [[Query::and([Query::equal('title', ['a']), Query::equal('count', [1])])]],
            'exists' => [[Query::exists(['title'])]],
            'select' => [[Query::select(['title'])]],
            'vector' => [[Query::vectorDot('embedding', [1.0, 2.0, 3.0])]],
            'distance' => [[Query::distanceLessThan('location', [1, 2], 10)]],
            'spatial' => [[Query::intersects('location', [1, 2])]],
            'aggregate' => [[Query::count()]],
            'group by' => [[Query::groupBy(['title'])]],
            'join' => [[Query::join('other', 'title', 'title')]],
            'a string' => [['equal("title", ["Dune"])']],
            'one other query among narrow ones' => [[Query::equal('title', ['Dune']), Query::select(['title']), Query::limit(1)]],
        ];
    }

    /**
     * @param  list<mixed>  $queries
     */
    #[DataProvider('otherLists')]
    public function testAnyOtherListIsLeftToTheDocumentsValidator(array $queries): void
    {
        $this->assertFalse(Narrow::accepts($queries));
        $this->assertNull($this->narrow($queries, true, true));
    }

    public function testAnAttributeThatIsNotADocumentIsLeftToTheDocumentsValidator(): void
    {
        $this->assertNull(Narrow::of([Query::equal('title', ['Dune'])], [['key' => 'title']], $this->bounds(), self::MAX_VALUES, true, true, true));
    }

    public function testEachListIsCheckedAgainstTheAttributesItWasGiven(): void
    {
        $queries = [Query::equal('title', ['Dune'])];
        $string = [Attribute::string(key: 'title', size: 64)];
        $integer = [Attribute::integer(key: 'title')];

        $this->assertTrue(Narrow::of($queries, $string, $this->bounds(), self::MAX_VALUES, true, true, true)?->isValid($queries));

        $refused = Narrow::of($queries, $integer, $this->bounds(), self::MAX_VALUES, true, true, true);
        $this->assertFalse($refused?->isValid($queries));
        $this->assertSame('Invalid query: Query value is invalid for attribute "title"', $refused->getDescription());

        $missing = Narrow::of($queries, [], $this->bounds(), self::MAX_VALUES, true, true, true);
        $this->assertFalse($missing?->isValid($queries));
        $this->assertSame('Invalid query: Attribute not found in schema: title', $missing->getDescription());
    }

    public function testAListItWasNotMadeOfIsCheckedByItsValidators(): void
    {
        $narrow = Narrow::of([Query::equal('title', ['Dune'])], $this->attributes(), $this->bounds(), self::MAX_VALUES, true, true, true);

        $this->assertFalse($narrow?->isValid([Query::limit(5)]));
        $this->assertSame('Invalid query method: limit', $narrow->getDescription());
        $this->assertFalse($narrow->isValid([Query::equal('count', [5])]));
        $this->assertSame('Invalid query: Attribute not found in schema: count', $narrow->getDescription());
        $this->assertFalse($narrow->isValid('not a list'));
        $this->assertSame('Queries must be an array', $narrow->getDescription());
        $this->assertTrue($narrow->isValid([Query::equal('title', ['Dune'])]));
    }

    public function testNothingAnEarlierListRegisteredReachesTheNextOne(): void
    {
        $narrow = Narrow::of([Query::orderAsc('title'), Query::equal('title', ['Dune'])], $this->attributes(), $this->bounds(), self::MAX_VALUES, true, true, true);

        $this->assertFalse($narrow?->isValid([Query::count('*', 'total'), Query::join('other', 'title', 'title', alias: 'o'), Query::orderAsc('total')]));
        $this->assertFalse($narrow->isValid([Query::orderAsc('total')]));
        $this->assertSame('Invalid query: Attribute not found in schema: total', $narrow->getDescription());

        $narrow->setJoinedCollections([new Document([Document::ID => 'other', 'attributes' => [Attribute::string(key: 'nickname', size: 16)]])]);
        $this->assertFalse($narrow->isValid([Query::join('other', 'title', 'nickname', alias: 'o'), Query::orderAsc('nickname')]));
        $this->assertFalse($narrow->isValid([Query::orderAsc('nickname')]));
        $this->assertSame('Invalid query: Attribute not found in schema: nickname', $narrow->getDescription());
    }

    /**
     * @param  list<mixed>  $queries
     */
    private function narrow(array $queries, bool $supportForAttributes, bool $orderRandom): ?Narrow
    {
        return Narrow::of($queries, $this->attributes(), $this->bounds(), self::MAX_VALUES, $supportForAttributes, true, $orderRandom);
    }

    private function documents(bool $supportForAttributes, bool $orderRandom): Documents
    {
        $bounds = $this->bounds();

        return new Documents(
            $this->attributes(),
            [new Index('title_fulltext', IndexType::Fulltext, ['title']), new Index('count_key', IndexType::Key, ['count'])],
            $bounds->idAttributeType,
            self::MAX_VALUES,
            $bounds->maxUIDLength,
            $bounds->minDateTime,
            $bounds->maxDateTime,
            $supportForAttributes,
            true,
            true,
            true,
            false,
            $orderRandom,
        );
    }

    private function bounds(): Bounds
    {
        return new Bounds(ColumnType::Integer->value, 36, new DateTime('0000-01-01'), new DateTime('9999-12-31 23:59:59'));
    }

    /**
     * @return list<Document>
     */
    private function attributes(): array
    {
        return [
            Attribute::string(key: 'title', size: 64),
            Attribute::integer(key: 'count'),
            Attribute::integer(key: 'unsigned', signed: false),
            Attribute::integer(key: 'big', size: 8),
            Attribute::bigInteger(key: 'huge'),
            Attribute::bigInteger(key: 'hugeUnsigned', signed: false),
            Attribute::float(key: 'price'),
            Attribute::boolean(key: 'active'),
            Attribute::datetime(key: 'published'),
            Attribute::string(key: 'tags', size: 32, array: true),
            Attribute::string(key: 'secret', size: 64, filters: ['encrypt']),
            Attribute::string(key: 'dup', size: 8),
            Attribute::integer(key: 'dup'),
            Attribute::object(key: 'meta'),
            Attribute::point(key: 'location'),
            Attribute::vector(key: 'embedding', size: 3),
            Attribute::integer(key: Document::ID),
            $this->relationship('author', RelationType::OneToOne, false, RelationSide::Child),
            $this->relationship('books', RelationType::OneToMany, true, RelationSide::Parent),
            $this->relationship('editor', RelationType::ManyToOne, true, RelationSide::Child),
            $this->relationship('readers', RelationType::ManyToMany, true, RelationSide::Parent),
            $this->relationship('owner', RelationType::ManyToOne, true, RelationSide::Parent),
        ];
    }

    private function relationship(string $key, RelationType $type, bool $twoWay, RelationSide $side): Attribute
    {
        return Attribute::relationship(key: $key, options: [
            'relatedCollection' => 'people',
            'relationType' => $type->value,
            'twoWay' => $twoWay,
            'twoWayKey' => $key.'Back',
            'onDelete' => 'restrict',
            'side' => $side->value,
        ]);
    }
}
