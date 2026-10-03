<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Queries;
use Utopia\Database\Validator\Queries\Document as DocumentQueries;
use Utopia\Database\Validator\Queries\Documents as DocumentsQueries;
use Utopia\Database\Validator\Query\Aggregate;
use Utopia\Database\Validator\Query\GroupBy;
use Utopia\Database\Validator\Query\Join;
use Utopia\Database\Validator\Query\Select;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;

/**
 * The query validators accept exactly the shapes the engines can run: a join condition compares
 * columns its tables have, `$collection` and, without shared tables, `$tenant` have no column to
 * aggregate or group, an encrypted joined attribute cannot be filtered, and each column of an
 * aggregation's result has a name of its own.
 */
final class JoinConditionColumnsTest extends TestCase
{
    /**
     * @return iterable<string, array{list<Query>}>
     */
    public static function joinsOverColumnsTheTablesHave(): iterable
    {
        $note = Query::join('notes', '$id', 'customerId', '=', 'note');

        yield 'main $id to a joined attribute' => [[$note]];
        yield 'an on condition under the join alias' => [[Query::leftJoin('notes', 'note', [Query::on('$id', 'note.customerId'), Query::on('$createdAt', '$updatedAt', '<')])]];
        yield 'a join after the join it names' => [[$note, Query::join('replies', 'note.$id', 'noteId', '=', 'reply')]];
        yield 'a join after a cross join it names' => [[Query::crossJoin('replies', 'reply'), Query::rightJoin('notes', 'reply.noteId', '$id', '=', 'note')]];
        yield 'internal attributes on both sides' => [[Query::join('notes', '$sequence', '$sequence', '<', 'note')]];
        yield 'a relationship that holds a column on the left' => [[Query::join('libraries', 'library', '$id', '=', 'lib')]];
        yield 'a relationship that holds a column on the right' => [[Query::leftJoin('books', '$id', 'owner', '=', 'book')]];
        yield 'a join without an alias' => [[Query::join('notes', '$id', 'customerId')]];
    }

    /**
     * @param  list<Query>  $joins
     */
    #[DataProvider('joinsOverColumnsTheTablesHave')]
    public function testJoinOverColumnsTheTablesHaveIsValid(array $joins): void
    {
        foreach ($this->validators() as $label => $validator) {
            $this->assertTrue($validator->isValid($joins), $label.': '.$validator->getDescription());
        }
    }

    /**
     * @return iterable<string, array{list<Query>, string}>
     */
    public static function joinsNamingNoColumn(): iterable
    {
        $note = Query::join('notes', '$id', 'customerId', '=', 'note');
        $notFound = 'Invalid query: Attribute not found in schema: ';
        $left = 'Invalid query: The left column of a join condition must belong to the main collection or to a join declared before it: ';
        $right = 'Invalid query: The right column of a join condition must belong to the joined collection: ';

        yield 'an unknown left column' => [[Query::join('notes', 'nothing', 'customerId', '=', 'note')], $notFound.'nothing'];
        yield 'an unknown right column' => [[Query::join('notes', '$id', 'nothing', '=', 'note')], $notFound.'nothing'];
        yield 'an unknown left column of an on condition' => [[Query::leftJoin('notes', 'note', [Query::on('nothing', 'customerId')])], $notFound.'nothing'];
        yield 'an unknown right column of an on condition' => [[Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId'), Query::on('$id', 'note.nothing')])], $notFound.'note.nothing'];
        yield 'an unknown column of an earlier join' => [[$note, Query::join('replies', 'note.nothing', 'noteId', '=', 'reply')], $notFound.'note.nothing'];
        yield 'an internal attribute a filter cannot compare' => [[Query::join('notes', '$permissions', 'customerId', '=', 'note')], $notFound.'$permissions'];
        yield 'a derived internal attribute' => [[Query::join('notes', '$id', '$collection', '=', 'note')], $notFound.'$collection'];
        yield 'a relationship without a column on the left' => [[Query::join('books', 'books', '$id', '=', 'book')], 'Invalid query: Cannot join on virtual relationship attribute: books'];
        yield 'a relationship without a column on the right' => [[Query::join('libraries', '$id', 'person', '=', 'lib')], 'Invalid query: Cannot join on virtual relationship attribute: person'];
        yield 'a join declared after it' => [[Query::join('replies', 'note.$id', 'noteId', '=', 'reply'), $note], $left.'note.$id'];
        yield 'its own alias on the left' => [[Query::join('notes', 'note.customerId', '$id', '=', 'note')], $left.'note.customerId'];
        yield 'an alias no join declares on the left' => [[Query::join('notes', 'other.$id', 'customerId', '=', 'note')], $left.'other.$id'];
        yield 'the main alias on the left' => [[Query::join('notes', Query::DEFAULT_ALIAS.'.$id', 'customerId', '=', 'note')], $left.Query::DEFAULT_ALIAS.'.$id'];
        yield 'an earlier join on the right' => [[$note, Query::join('replies', '$id', 'note.$id', '=', 'reply')], $right.'note.$id'];
        yield 'the main alias on the right' => [[Query::join('notes', '$id', Query::DEFAULT_ALIAS.'.name', '=', 'note')], $right.Query::DEFAULT_ALIAS.'.name'];
        yield 'no right column' => [[new Query(Method::Join, 'notes', ['$id', '='])], 'Invalid query: Join ON requires left and right columns'];
        yield 'a right column that is not a string' => [[new Query(Method::LeftJoin, 'notes', ['$id', '=', 5, 'note'])], 'Invalid query: Join ON requires left and right columns'];
        yield 'an operator no engine compares with' => [[Query::join('notes', '$id', 'customerId', '~', 'note')], 'Invalid query: Invalid join operator: ~'];
    }

    /**
     * @param  list<Query>  $joins
     */
    #[DataProvider('joinsNamingNoColumn')]
    public function testJoinNamingNoColumnIsInvalid(array $joins, string $message): void
    {
        foreach ($this->validators() as $label => $validator) {
            $this->assertFalse($validator->isValid($joins), $label.': the join was accepted');
            $this->assertSame($message, $validator->getDescription(), $label);
        }
    }

    public function testColumnsOfACollectionTheValidatorDoesNotKnowAreNotChecked(): void
    {
        $validator = new Queries([new Join()]);

        $this->assertTrue($validator->isValid([Query::join('orders', 'user_id', 'id')]), $validator->getDescription());
        $this->assertTrue($validator->isValid([Query::join('orders', 'user_id', 'id', '=', 'ord'), Query::join('items', 'ord.anything', 'orderId', '=', 'item')]), $validator->getDescription());

        $this->assertFalse($validator->isValid([Query::join('items', 'ord.anything', 'orderId', '=', 'item'), Query::join('orders', 'user_id', 'id', '=', 'ord')]), 'the order of the joins is still checked');
        $this->assertSame('Invalid query: The left column of a join condition must belong to the main collection or to a join declared before it: ord.anything', $validator->getDescription());
    }

    public function testSchemalessJoinsCheckOnlyWhichTablesAColumnBelongsTo(): void
    {
        $validator = new Queries([new Join($this->customers(), supportForAttributes: false)]);
        $validator->setJoinedCollections($this->collections());

        $this->assertTrue($validator->isValid([Query::join('notes', 'anything', 'whatever', '=', 'note')]), $validator->getDescription());

        $this->assertFalse($validator->isValid([Query::join('notes', 'reply.anything', 'whatever', '=', 'note')]));
        $this->assertSame('Invalid query: The left column of a join condition must belong to the main collection or to a join declared before it: reply.anything', $validator->getDescription());
    }

    public function testDocumentQueriesCheckTheColumnsOfAJoinCondition(): void
    {
        $validator = new DocumentQueries($this->customers());
        $validator->setJoinedCollections($this->collections());

        $this->assertTrue($validator->isValid([Query::leftJoin('notes', 'note', [Query::on('$id', 'customerId')])]), $validator->getDescription());

        $this->assertFalse($validator->isValid([Query::leftJoin('notes', 'note', [Query::on('$id', 'nothing')])]));
        $this->assertSame('Invalid query: Attribute not found in schema: nothing', $validator->getDescription());
    }

    public function testValidatorForgetsTheJoinsOfThePreviousQuerySet(): void
    {
        $validator = $this->validators()['documents'];

        $this->assertTrue($validator->isValid([Query::join('notes', '$id', 'customerId', '=', 'note')]), $validator->getDescription());
        $this->assertFalse($validator->isValid([Query::join('replies', 'note.$id', 'noteId', '=', 'reply')]), 'an alias of the previous query set is not declared in this one');
    }

    /**
     * @return iterable<string, array{list<Query>, string, bool}>
     */
    public static function internalAttributesWithoutAColumn(): iterable
    {
        $note = Query::join('notes', '$id', 'customerId', '=', 'note');

        yield '$collection counted' => [[Query::count('$collection', 'total')], '$collection', false];
        yield '$collection grouped' => [[Query::count('*', 'rows'), Query::groupBy(['$collection'])], '$collection', false];
        yield '$tenant counted' => [[Query::count('$tenant', 'total')], '$tenant', true];
        yield '$tenant grouped' => [[Query::count('*', 'rows'), Query::groupBy(['$tenant'])], '$tenant', true];
        yield '$tenant selected' => [[Query::select(['name', '$tenant'])], '$tenant', true];
        yield 'a joined $tenant counted' => [[$note, Query::count('note.$tenant', 'total')], 'note.$tenant', true];
        yield 'a joined $tenant grouped' => [[$note, Query::count('*', 'rows'), Query::groupBy(['note.$tenant'])], 'note.$tenant', true];
        yield 'a joined $tenant selected' => [[$note, Query::select(['name', 'note.$tenant'])], 'note.$tenant', true];
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('internalAttributesWithoutAColumn')]
    public function testInternalAttributeWithoutAColumnIsInvalid(array $queries, string $attribute, bool $validUnderSharedTables): void
    {
        $validator = $this->documents(sharedTables: false);

        $this->assertFalse($validator->isValid($queries), 'accepted without shared tables');
        $this->assertSame('Invalid query: Attribute not found in schema: '.$attribute, $validator->getDescription());

        $shared = $this->documents(sharedTables: true);
        $this->assertSame($validUnderSharedTables, $shared->isValid($queries), $shared->getDescription());
    }

    public function testCollectionStaysSelectableOnTheMainCollection(): void
    {
        foreach ([false, true] as $sharedTables) {
            $validator = $this->documents($sharedTables);

            $this->assertTrue($validator->isValid([Query::select(['name', '$collection'])]), $validator->getDescription());
            $this->assertFalse($validator->isValid([Query::join('notes', '$id', 'customerId', '=', 'note'), Query::select(['note.$collection'])]));
        }
    }

    public function testTenantIsRejectedByValidatorsBuiltWithoutSharedTables(): void
    {
        $attributes = $this->customers();

        $this->assertFalse((new Aggregate($attributes))->isValid(Query::count('$tenant', 'total')));
        $this->assertTrue((new Aggregate($attributes, sharedTables: true))->isValid(Query::count('$tenant', 'total')));
        $this->assertFalse((new Aggregate($attributes, sharedTables: true))->isValid(Query::count('$collection', 'total')));
        $this->assertFalse((new GroupBy($attributes))->isValid(Query::groupBy(['$tenant'])));
        $this->assertTrue((new GroupBy($attributes, sharedTables: true))->isValid(Query::groupBy(['$tenant'])));
        $this->assertFalse((new Select($attributes))->isValid(Query::select(['$tenant'])));
        $this->assertTrue((new Select($attributes, sharedTables: true))->isValid(Query::select(['$tenant'])));
    }

    public function testEncryptedJoinedAttributeCannotBeFiltered(): void
    {
        $join = Query::join('notes', '$id', 'customerId', '=', 'note');
        $validator = $this->documents(sharedTables: false);

        foreach ([
            Query::equal('note.secret', ['x']),
            Query::isNull('note.secret'),
            Query::and([Query::equal('note.body', ['x']), Query::startsWith('note.secret', 'x')]),
        ] as $filter) {
            $this->assertFalse($validator->isValid([$join, $filter]), 'an encrypted joined attribute was filtered');
            $this->assertSame('Invalid query: Cannot query encrypted attribute: note.secret', $validator->getDescription());
        }

        $this->assertTrue($validator->isValid([$join, Query::select(['name', 'note.secret']), Query::orderAsc('note.secret')]), $validator->getDescription());
        $this->assertFalse($validator->isValid([Query::equal('secret', ['x'])]));
        $this->assertSame('Invalid query: Cannot query encrypted attribute: secret', $validator->getDescription());
    }

    /**
     * @return iterable<string, array{list<Query>, string|null}>
     */
    public static function aggregateAliases(): iterable
    {
        $grouped = static fn (string $alias, string $attribute): string => 'Invalid query: Aggregate alias "'.$alias.'" is the name the groupBy attribute "'.$attribute.'" is returned under';
        $join = Query::join('notes', '$id', 'customerId', '=', 'note');

        yield 'a grouped attribute' => [[Query::count('*', 'name'), Query::groupBy(['name'])], $grouped('name', 'name')];
        yield 'a grouped joined attribute' => [[$join, Query::count('*', 'body'), Query::groupBy(['name', 'note.body'])], $grouped('body', 'note.body')];
        yield 'the column of a grouped internal attribute' => [[Query::count('*', '_createdAt'), Query::groupBy(['$createdAt'])], $grouped('_createdAt', '$createdAt')];
        yield 'another aggregate' => [[Query::count('*', 'total'), Query::sum('score', 'total')], 'Invalid query: Aggregate alias "total" is given to more than one aggregate'];
        yield 'an attribute that is not grouped' => [[Query::sum('score', 'name'), Query::groupBy(['score'])], null];
        yield 'aliases of their own' => [[$join, Query::count('*', 'rows'), Query::sum('note.score', 'score'), Query::groupBy(['name', 'note.body'])], null];
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('aggregateAliases')]
    public function testAggregateAliasNamesOneColumnOfTheResult(array $queries, ?string $message): void
    {
        $validator = $this->documents(sharedTables: false);

        $this->assertSame($message === null, $validator->isValid($queries), $validator->getDescription());
        if ($message !== null) {
            $this->assertSame($message, $validator->getDescription());
        }
    }

    /**
     * @return array<string, Queries>
     */
    private function validators(): array
    {
        $documents = $this->documents(sharedTables: false);

        $document = new DocumentQueries($this->customers());
        $document->setJoinedCollections($this->collections());

        $joins = new Queries([new Join($this->customers())]);
        $joins->setJoinedCollections($this->collections());

        return ['documents' => $documents, 'document' => $document, 'join' => $joins];
    }

    private function documents(bool $sharedTables): DocumentsQueries
    {
        $validator = new DocumentsQueries(
            attributes: $this->customers(),
            indexes: [],
            idAttributeType: ColumnType::Integer->value,
            supportForJoins: true,
            supportForAggregations: true,
            sharedTables: $sharedTables,
        );
        $validator->setJoinedCollections($this->collections());

        return $validator;
    }

    /**
     * @return array<Document>
     */
    private function customers(): array
    {
        return [
            $this->attribute('name', ColumnType::String),
            $this->attribute('score', ColumnType::Integer),
            $this->attribute('secret', ColumnType::String, filters: ['encrypt']),
            $this->relationship('library', RelationType::OneToOne, RelationSide::Parent),
            $this->relationship('books', RelationType::OneToMany, RelationSide::Parent),
        ];
    }

    /**
     * @return array<Document>
     */
    private function collections(): array
    {
        return [
            $this->collection('notes', [
                $this->attribute('customerId', ColumnType::String),
                $this->attribute('body', ColumnType::String),
                $this->attribute('score', ColumnType::Integer),
                $this->attribute('secret', ColumnType::String, filters: ['encrypt']),
            ]),
            $this->collection('replies', [
                $this->attribute('noteId', ColumnType::String),
            ]),
            $this->collection('libraries', [
                $this->attribute('name', ColumnType::String),
                $this->relationship('person', RelationType::OneToOne, RelationSide::Child),
            ]),
            $this->collection('books', [
                $this->attribute('title', ColumnType::String),
                $this->relationship('owner', RelationType::OneToMany, RelationSide::Child),
            ]),
        ];
    }

    /**
     * @param  array<Document>  $attributes
     */
    private function collection(string $id, array $attributes): Document
    {
        return new Document(['$id' => $id, 'attributes' => $attributes, 'indexes' => []]);
    }

    /**
     * @param  list<string>  $filters
     */
    private function attribute(string $key, ColumnType $type, array $filters = []): Document
    {
        return new Document([
            '$id' => $key,
            'key' => $key,
            'type' => $type->value,
            'size' => $type === ColumnType::String ? 256 : 0,
            'required' => false,
            'signed' => true,
            'array' => false,
            'filters' => $filters,
        ]);
    }

    private function relationship(string $key, RelationType $type, RelationSide $side): Document
    {
        return new Document([
            '$id' => $key,
            'key' => $key,
            'type' => ColumnType::Relationship->value,
            'size' => 0,
            'required' => false,
            'signed' => true,
            'array' => false,
            'filters' => [],
            'options' => [
                'relatedCollection' => 'related',
                'relationType' => $type->value,
                'twoWay' => false,
                'twoWayKey' => 'back',
                'onDelete' => 'restrict',
                'side' => $side->value,
            ],
        ]);
    }
}
