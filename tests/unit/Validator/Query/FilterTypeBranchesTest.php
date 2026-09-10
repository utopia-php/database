<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Query\Schema\ColumnType;

/**
 * Covers the attribute types whose check reads the value itself, rather than
 * only the column's declared type. These are the branches that cannot be
 * hoisted out of the per-value loop, so they are the ones a reader has to trust
 * stayed put.
 */
class FilterTypeBranchesTest extends TestCase
{
    protected Filter $validator;

    protected function setUp(): void
    {
        $this->validator = new Filter(
            attributes: [
                new Document([
                    '$id' => 'vector',
                    'key' => 'vector',
                    'type' => ColumnType::Vector->value,
                    'array' => false,
                    'size' => 3,
                ]),
                new Document([
                    '$id' => 'point',
                    'key' => 'point',
                    'type' => ColumnType::Point->value,
                    'array' => false,
                ]),
                new Document([
                    '$id' => 'object',
                    'key' => 'object',
                    'type' => ColumnType::Object->value,
                    'array' => false,
                ]),
                new Document([
                    '$id' => 'text',
                    'key' => 'text',
                    'type' => ColumnType::String->value,
                    'array' => false,
                ]),
            ],
            idAttributeType: ColumnType::Integer->value,
            supportUnsignedBigInt: true,
        );
    }

    public function test_vector_requires_a_numeric_array_of_the_declared_size(): void
    {
        $this->assertTrue($this->validator->isValid(Query::equal('vector', [[1.0, 2.0, 3.0]])));

        $this->assertFalse($this->validator->isValid(Query::equal('vector', ['not-an-array'])));
        $this->assertSame('Vector query value must be an array', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::equal('vector', [[1.0, 'two', 3.0]])));
        $this->assertSame('Vector query value must contain only numeric values', $this->validator->getDescription());

        $this->assertFalse($this->validator->isValid(Query::equal('vector', [[1.0, 2.0]])));
        $this->assertSame('Vector query value must have 3 elements', $this->validator->getDescription());
    }

    public function test_spatial_requires_an_array_value(): void
    {
        $this->assertTrue($this->validator->isValid(Query::equal('point', [[1.0, 2.0]])));

        $this->assertFalse($this->validator->isValid(Query::equal('point', ['1,2'])));
        $this->assertSame('Spatial data must be an array', $this->validator->getDescription());
    }

    public function test_object_containment_rejects_mixed_key_arrays(): void
    {
        $this->assertTrue($this->validator->isValid(Query::equal('object', [['a' => 1]])));
        $this->assertTrue($this->validator->isValid(Query::equal('object', ['plain-string'])), 'a scalar is not an object shape to check');

        // A map and a list at once is the shape the check exists to refuse:
        // it cannot be encoded as either a JSON object or a JSON array.
        $this->assertFalse($this->validator->isValid(Query::equal('object', [['a' => 1, 0 => 'b']])));
        $this->assertSame(
            'Invalid object query structure for attribute "object"',
            $this->validator->getDescription(),
        );

        $this->assertFalse($this->validator->isValid(Query::equal('object', [['nested' => ['x' => 1, 0 => 'y']]])), 'the check recurses');
    }

    public function test_a_dotted_object_path_is_validated_as_a_string(): void
    {
        $this->assertTrue($this->validator->isValid(Query::equal('object.nested', ['a-string'])));
    }

    public function test_every_value_is_checked_not_just_the_first(): void
    {
        $this->assertFalse($this->validator->isValid(Query::equal('vector', [[1.0, 2.0, 3.0], 'not-an-array'])));
        $this->assertSame('Vector query value must be an array', $this->validator->getDescription());
    }
}
