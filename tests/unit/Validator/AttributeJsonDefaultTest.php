<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;
use Tests\Unit\Support\Profiles;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Filter;
use Utopia\Database\Validator\AttributeDefinition;

class AttributeJsonDefaultTest extends TestCase
{
    private AttributeDefinition $validator;

    #[\Override]
    protected function setUp(): void
    {
        $this->validator = new AttributeDefinition(
            attributes: [],
            profile: Profiles::of(string: 16777216, varchar: 65535, integer: PHP_INT_MAX),
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

        $this->validator->isValid(Attribute::string(key: 'meta', size: 65535, default: $default, filters: [Filter::Json]));
    }

    public function test_structured_default_must_be_json_encodable(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Default value of json attribute "meta" is not JSON-encodable: Malformed UTF-8 characters, possibly incorrectly encoded');

        $this->validator->isValid(Attribute::string(key: 'meta', size: 65535, default: ['name' => "\xB1\x31"], filters: [Filter::Json]));
    }

    public function test_json_filter_does_not_exempt_non_string_types(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Cannot set an array default value for a non-array attribute');

        $this->validator->isValid(Attribute::integer(key: 'count', default: [], filters: [Filter::Json]));
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
        $this->assertTrue($this->validator->isValid(Attribute::string(key: 'meta', size: 65535, default: $default, filters: [Filter::Json])));
    }
}
