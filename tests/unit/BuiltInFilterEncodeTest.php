<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Vector;

/**
 * The built-in json/object/vector encode filters serialise with json_encode(),
 * which returns false when it cannot encode the value. A false where a string
 * belongs is then rejected by Structure with a message about the attribute's
 * declared type and length, which says nothing about the serialisation failure
 * that actually caused it. These tests pin the real cause being reported.
 */
class BuiltInFilterEncodeTest extends TestCase
{
    private Database $database;

    protected function setUp(): void
    {
        $this->database = (new Database(new DatabaseMemory(), new Cache(new None())))
            ->setDatabase('utopiaTests')
            ->setNamespace('builtin_filter_' . \uniqid());
    }

    /**
     * Malformed UTF-8 anywhere in the payload fails json_encode().
     */
    private function malformedUtf8(): string
    {
        return "bad \xC3\x28 utf8";
    }

    /**
     * @param array<string> $filters
     */
    private function collection(string $type, array $filters): Document
    {
        return new Document([
            '$id' => 'logs',
            'attributes' => [
                new Document([
                    '$id' => 'data',
                    'type' => $type,
                    'size' => 5000000,
                    'required' => false,
                    'array' => false,
                    'filters' => $filters,
                ]),
            ],
        ]);
    }

    /**
     * @param array<string> $filters
     */
    private function encode(string $type, array $filters, mixed $value): mixed
    {
        return $this->database
            ->encode($this->collection($type, $filters), new Document(['data' => $value]))
            ->getAttribute('data');
    }

    public function testJsonFilterReportsTheSerialisationFailure(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Failed to encode attribute "data": Malformed UTF-8 characters');

        $this->encode(Database::VAR_STRING, ['json'], ['message' => $this->malformedUtf8()]);
    }

    public function testObjectFilterReportsTheSerialisationFailure(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Failed to encode attribute "data": Malformed UTF-8 characters');

        $this->encode(Database::VAR_OBJECT, [Database::VAR_OBJECT], ['message' => $this->malformedUtf8()]);
    }

    /**
     * A vector holding INF or NAN is not encodable either, but the component is
     * what the caller needs to hear about, so the filter leaves the value for
     * the Vector validator rather than raising a JSON error.
     */
    public function testVectorFilterLeavesNonFiniteComponentsToTheValidator(): void
    {
        $validator = new Vector(2);

        foreach ([[1.0, \INF], [1.0, -\INF], [1.0, \NAN]] as $vector) {
            // Handed back as-is rather than encoded, which is what stops the
            // filter from raising a JSON error for this case
            $this->assertIsArray($this->encode(Database::VAR_VECTOR, [Database::VAR_VECTOR], $vector));

            $this->assertFalse($validator->isValid($vector), 'Non-finite components must not validate');
        }

        // The wording the caller ends up with, which the e2e vector tests assert on
        $this->assertStringContainsString('numeric', $validator->getDescription());

        // Finite components are unaffected
        $this->assertTrue($validator->isValid([1e38, -1e38]));
    }

    /**
     * json_encode() also fails past its nesting limit, which is the other way a
     * caller-supplied payload turns into a false.
     */
    public function testJsonFilterReportsExceedingTheNestingLimit(): void
    {
        $deep = 'leaf';
        for ($i = 0; $i < 600; $i++) {
            $deep = [$deep];
        }

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Failed to encode attribute "data": Maximum stack depth exceeded');

        $this->encode(Database::VAR_STRING, ['json'], ['nested' => $deep]);
    }

    /**
     * The whole write path, as the caller sees it: the failure must not be
     * reported as the attribute having the wrong type or being too long.
     */
    public function testCreateDocumentDoesNotBlameTheAttributeTypeOrLength(): void
    {
        $this->database->create();
        $this->database->createCollection('logs');
        $this->database->createAttribute('logs', 'data', Database::VAR_STRING, 5000000, false, null, true, false, null, [], ['json']);

        try {
            $this->database->createDocument('logs', new Document([
                '$permissions' => [Permission::read(Role::any())],
                'data' => ['message' => $this->malformedUtf8()],
            ]));
            $this->fail('Expected the write to fail');
        } catch (StructureException $e) {
            $this->fail('Serialisation failure reported as a structure error: ' . $e->getMessage());
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('Failed to encode attribute "data": Malformed UTF-8 characters', $e->getMessage());
        }
    }

    public function testEncodableValuesStillEncode(): void
    {
        $this->assertSame(
            '{"message":"fine"}',
            $this->encode(Database::VAR_STRING, ['json'], ['message' => 'fine'])
        );

        $this->assertSame(
            '{"message":"fine"}',
            $this->encode(Database::VAR_OBJECT, [Database::VAR_OBJECT], ['message' => 'fine'])
        );

        $this->assertSame(
            '[1.5,2]',
            $this->encode(Database::VAR_VECTOR, [Database::VAR_VECTOR], [1.5, 2])
        );
    }
}
