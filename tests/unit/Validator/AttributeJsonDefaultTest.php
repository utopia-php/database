<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Validator\Attribute as AttributeValidator;

class AttributeJsonDefaultTest extends TestCase
{
    private AttributeValidator $validator;

    protected function setUp(): void
    {
        $this->validator = new AttributeValidator(
            attributes: [],
            maxStringLength: 16777216,
            maxVarcharLength: 65535,
            maxIntLength: PHP_INT_MAX,
        );
    }

    /**
     * @return array<string, array{mixed, string}>
     */
    public static function scalarDefaults(): array
    {
        return [
            'integer' => [12345, 'Default value 12345 does not match given type string'],
            'float' => [1.5, 'Default value 1.5 does not match given type string'],
            'boolean' => [true, 'Default value true does not match given type string'],
        ];
    }

    #[DataProvider('scalarDefaults')]
    public function test_scalar_default_must_match_the_storage_type(mixed $default, string $message): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage($message);

        $this->validator->isValid(Attribute::string(key: 'meta', size: 65535, default: $default, filters: ['json']));
    }

    public function test_structured_default_must_be_json_encodable(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Default value of json attribute "meta" is not JSON-encodable: Malformed UTF-8 characters, possibly incorrectly encoded');

        $this->validator->isValid(Attribute::string(key: 'meta', size: 65535, default: ['name' => "\xB1\x31"], filters: ['json']));
    }

    public function test_json_filter_does_not_exempt_non_string_types(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Cannot set an array default value for a non-array attribute');

        $this->validator->isValid(Attribute::integer(key: 'count', default: [], filters: ['json']));
    }

    /**
     * @return array<string, array{mixed}>
     */
    public static function jsonDocuments(): array
    {
        return [
            'empty list' => [[]],
            'list' => [['a', 'b']],
            'map' => [['cost' => 12, 'memory' => 65536]],
            'object' => [new stdClass()],
            'document' => [new Document(['cost' => 12])],
            'json text' => ['{}'],
            'empty text' => [''],
        ];
    }

    #[DataProvider('jsonDocuments')]
    public function test_json_document_default_is_valid(mixed $default): void
    {
        $this->assertTrue($this->validator->isValid(Attribute::string(key: 'meta', size: 65535, default: $default, filters: ['json'])));
    }
}
