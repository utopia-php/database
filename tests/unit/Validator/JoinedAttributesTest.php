<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Queries\Documents;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

class JoinedAttributesTest extends TestCase
{
    private const string AMBIGUOUS_AMOUNT = 'Invalid query: Attribute "amount" is ambiguous across joins; qualify it with a join alias';

    private Document $customers;

    private Document $orders;

    private Document $refunds;

    private Document $notes;

    private Document $profiles;

    protected function setUp(): void
    {
        $this->customers = $this->collection('customers', [
            $this->attribute('name', ColumnType::String),
            $this->attribute('visits', ColumnType::Integer),
        ]);
        $this->orders = $this->collection('orders', [
            $this->attribute('customerId', ColumnType::String),
            $this->attribute('amount', ColumnType::Integer),
            $this->attribute('status', ColumnType::String),
            $this->attribute('memo', ColumnType::String),
            $this->attribute('customer', ColumnType::Relationship),
        ]);
        $this->refunds = $this->collection('refunds', [
            $this->attribute('customerId', ColumnType::String),
            $this->attribute('amount', ColumnType::Integer),
        ]);
        $this->notes = $this->collection('notes', [
            $this->attribute('customerId', ColumnType::String),
            $this->attribute('body', ColumnType::String),
        ], [
            new Document([
                '$id' => 'body_fulltext',
                'key' => 'body_fulltext',
                'type' => IndexType::Fulltext->value,
                'attributes' => ['body'],
            ]),
        ]);
        $this->profiles = $this->collection('profiles', [
            $this->attribute('customerId', ColumnType::String),
            $this->attribute('visits', ColumnType::Integer),
        ]);
    }

    public function testBareAttributeOfTheMainCollectionStaysValidWhenAJoinDeclaresItToo(): void
    {
        $validator = $this->validator([$this->profiles]);

        $this->assertTrue($validator->isValid([
            Query::join('profiles', '$id', 'customerId', '=', 'profile'),
            Query::sum('visits', 'total'),
            Query::groupBy(['name', 'visits']),
        ]), $validator->getDescription());
    }

    public function testBareAttributeResolvesToTheOneJoinThatDeclaresIt(): void
    {
        $validator = $this->validator([$this->notes, $this->orders]);

        $this->assertTrue($validator->isValid([
            Query::join('notes', '$id', 'customerId', '=', 'note'),
            Query::join('orders', '$id', 'customerId', '=', 'purchase'),
            Query::sum('amount', 'total'),
            Query::groupBy(['status']),
        ]), $validator->getDescription());

        $this->assertTrue($validator->isValid([
            Query::join('notes', '$id', 'customerId'),
            Query::join('orders', '$id', 'customerId'),
            Query::avg('amount', 'average'),
            Query::groupBy(['status']),
        ]), 'a join without an alias still declares its attributes: '.$validator->getDescription());
    }

    public function testBareAttributeNoCollectionDeclaresIsNotFound(): void
    {
        $validator = $this->validator([$this->orders]);

        $this->assertFalse($validator->isValid([
            Query::leftJoin('orders', '$id', 'customerId', '=', 'j'),
            Query::groupBy(['anything_at_all']),
        ]));
        $this->assertSame('Invalid query: Attribute not found in schema: anything_at_all', $validator->getDescription());

        $this->assertFalse($validator->isValid([
            Query::leftJoin('orders', '$id', 'customerId', '=', 'j'),
            Query::sum('also_anything', 'total'),
        ]));
        $this->assertSame('Invalid query: Attribute not found in schema: also_anything', $validator->getDescription());
    }

    public function testBareAttributeSeveralJoinsDeclareIsAmbiguous(): void
    {
        $validator = $this->validator([$this->orders, $this->refunds]);
        $joins = [
            Query::join('orders', '$id', 'customerId', '=', 'alpha'),
            Query::join('refunds', '$id', 'customerId', '=', 'beta'),
        ];

        $this->assertFalse($validator->isValid([...$joins, Query::sum('amount', 'total')]));
        $this->assertSame(self::AMBIGUOUS_AMOUNT, $validator->getDescription());

        $this->assertFalse($validator->isValid([...$joins, Query::groupBy(['amount'])]));
        $this->assertSame(self::AMBIGUOUS_AMOUNT, $validator->getDescription());

        $this->assertTrue($validator->isValid([
            ...$joins,
            Query::sum('beta.amount', 'total'),
            Query::groupBy(['alpha.amount']),
        ]), $validator->getDescription());
    }

    public function testCollectionJoinedUnderTwoAliasesMakesItsAttributesAmbiguous(): void
    {
        $validator = $this->validator([$this->orders]);
        $joins = [
            Query::join('orders', '$id', 'customerId', '=', 'first'),
            Query::leftJoin('orders', '$id', 'customerId', '=', 'second'),
        ];

        $this->assertFalse($validator->isValid([...$joins, Query::max('amount', 'largest')]));
        $this->assertSame(self::AMBIGUOUS_AMOUNT, $validator->getDescription());

        $this->assertTrue($validator->isValid([
            ...$joins,
            Query::max('first.amount', 'largest'),
            Query::min('second.amount', 'smallest'),
        ]), $validator->getDescription());
    }

    public function testBareAttributeCannotResolveThroughAJoinWhoseCollectionIsUnknown(): void
    {
        $validator = $this->validator();

        $this->assertFalse($validator->isValid([
            Query::join('orders', '$id', 'customerId', '=', 'purchase'),
            Query::sum('amount', 'total'),
        ]));
        $this->assertSame('Invalid query: Attribute not found in schema: amount', $validator->getDescription());

        $this->assertTrue($validator->isValid([
            Query::join('orders', '$id', 'customerId', '=', 'purchase'),
            Query::sum('purchase.amount', 'total'),
        ]), $validator->getDescription());
    }

    public function testJoinedRelationshipAttributeIsNotResolvedByItsBareName(): void
    {
        $validator = $this->validator([$this->orders]);

        $this->assertFalse($validator->isValid([
            Query::join('orders', '$id', 'customerId', '=', 'purchase'),
            Query::groupBy(['customer']),
        ]));
        $this->assertSame('Invalid query: Attribute not found in schema: customer', $validator->getDescription());
    }

    public function testInternalAttributesResolveToTheMainCollection(): void
    {
        $validator = $this->validator([$this->orders]);

        $this->assertTrue($validator->isValid([
            Query::leftJoin('orders', '$id', 'customerId', '=', 'purchase'),
            Query::count('$id', 'customers'),
            Query::groupBy(['$createdAt']),
        ]), $validator->getDescription());
    }

    public function testJoinedCollectionsDoNotWidenAQuerySetWithoutJoins(): void
    {
        $validator = $this->validator([$this->orders]);

        $this->assertTrue($validator->isValid([
            Query::join('orders', '$id', 'customerId', '=', 'purchase'),
            Query::sum('amount', 'total'),
        ]), $validator->getDescription());

        $this->assertFalse($validator->isValid([Query::sum('amount', 'total')]));
        $this->assertSame('Invalid query: Attribute not found in schema: amount', $validator->getDescription());

        $this->assertFalse($validator->isValid([Query::groupBy(['status'])]));
        $this->assertSame('Invalid query: Attribute not found in schema: status', $validator->getDescription());
    }

    /**
     * @param  array<Document>  $joinedCollections
     */
    private function validator(array $joinedCollections = []): Documents
    {
        /** @var array<Document> $attributes */
        $attributes = $this->customers->getAttribute('attributes', []);

        $validator = new Documents(
            attributes: $attributes,
            indexes: [],
            idAttributeType: ColumnType::Integer->value,
            supportForJoins: true,
            supportForAggregations: true,
        );

        if ($joinedCollections !== []) {
            $validator->setJoinedCollections($joinedCollections);
        }

        return $validator;
    }

    /**
     * @param  array<Document>  $attributes
     * @param  array<Document>  $indexes
     */
    private function collection(string $id, array $attributes, array $indexes = []): Document
    {
        return new Document([
            '$id' => $id,
            'attributes' => $attributes,
            'indexes' => $indexes,
        ]);
    }

    private function attribute(string $key, ColumnType $type): Document
    {
        return new Document([
            '$id' => $key,
            'key' => $key,
            'type' => $type->value,
            'size' => $type === ColumnType::String ? 256 : 0,
            'required' => false,
            'signed' => true,
            'array' => false,
            'filters' => [],
        ]);
    }
}
