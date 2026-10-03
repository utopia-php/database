<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Validator\ObjectValidator;

class ObjectTest extends TestCase
{
    public function test_valid_associative_objects(): void
    {
        $validator = new ObjectValidator();

        $this->assertTrue($validator->isValid(['key' => 'value']));
        $this->assertTrue($validator->isValid([
            'a' => [
                'b' => [
                    'c' => 123,
                ],
            ],
        ]));

        $this->assertTrue($validator->isValid([
            'author' => 'Arnab',
            'metadata' => [
                'rating' => 4.5,
                'info' => [
                    'category' => 'science',
                ],
            ],
        ]));

        $this->assertTrue($validator->isValid([
            'key1' => null,
            'key2' => ['nested' => null],
        ]));

        $this->assertTrue($validator->isValid([
            'meta' => (object) ['x' => 1],
        ]));

        $this->assertTrue($validator->isValid([
            'a' => 1,
            2 => 'b',
        ]));

    }

    public function test_invalid_structures(): void
    {
        $validator = new ObjectValidator();

        $this->assertFalse($validator->isValid(['a', 'b', 'c']));

        $this->assertFalse($validator->isValid('not an array'));

        $this->assertFalse($validator->isValid([
            0 => 'value',
        ]));
    }

    public function test_scalar_json_strings_are_not_objects(): void
    {
        $validator = new ObjectValidator();

        $this->assertFalse($validator->isValid('123'), 'a JSON number is not an object');
        $this->assertFalse($validator->isValid('0'), 'a falsy JSON number is not an object');
        $this->assertFalse($validator->isValid('true'), 'a JSON boolean is not an object');
        $this->assertFalse($validator->isValid('null'), 'JSON null is not an object');
        $this->assertFalse($validator->isValid('"str"'), 'a JSON string is not an object');
        $this->assertFalse($validator->isValid('""'), 'an empty JSON string is not an object');
        $this->assertFalse($validator->isValid('[1, 2]'), 'a JSON list is not an object');
    }

    public function test_json_object_strings_are_objects(): void
    {
        $validator = new ObjectValidator();

        $this->assertTrue($validator->isValid('{"a": 1}'));
        $this->assertTrue($validator->isValid('{}'));
        $this->assertTrue($validator->isValid('[]'), 'an empty JSON array matches the empty-array case');
    }

    public function test_empty_cases(): void
    {
        $validator = new ObjectValidator();

        $this->assertTrue($validator->isValid([]));
        $this->assertTrue($validator->isValid(new \stdClass()));

        $this->assertFalse($validator->isValid('sldfjsdlfj'));
    }
}
