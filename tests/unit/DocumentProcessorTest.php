<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\DocumentProcessor;

/**
 * Test that DocumentProcessor produces identical results to legacy decode+casting
 */
class DocumentProcessorTest extends TestCase
{
    private Database $database;
    private Document $collection;
    private DocumentProcessor $processor;

    public function setUp(): void
    {
        // Create a mock collection with various attribute types
        $this->collection = new Document([
            '$id' => 'test_collection',
            'attributes' => [
                [
                    '$id' => 'name',
                    'type' => Database::VAR_STRING,
                    'size' => 255,
                    'required' => false,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => 'age',
                    'type' => Database::VAR_INTEGER,
                    'size' => 0,
                    'required' => false,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => 'active',
                    'type' => Database::VAR_BOOLEAN,
                    'size' => 0,
                    'required' => false,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                ],
                [
                    '$id' => 'tags',
                    'type' => Database::VAR_STRING,
                    'size' => 255,
                    'required' => false,
                    'signed' => true,
                    'array' => true,
                    'filters' => ['json'],
                ],
                [
                    '$id' => 'metadata',
                    'type' => Database::VAR_STRING,
                    'size' => 16777216,
                    'required' => false,
                    'signed' => true,
                    'array' => false,
                    'filters' => ['json'],
                ],
                [
                    '$id' => 'created_at',
                    'type' => Database::VAR_DATETIME,
                    'size' => 0,
                    'required' => false,
                    'signed' => false,
                    'array' => false,
                    'filters' => ['datetime'],
                ],
                [
                    '$id' => 'scores',
                    'type' => Database::VAR_FLOAT,
                    'size' => 0,
                    'required' => false,
                    'signed' => true,
                    'array' => true,
                    'filters' => [],
                ],
            ],
        ]);

        $this->processor = new DocumentProcessor();

        // Use reflection to access private methods for comparison
        $this->database = $this->createPartialMock(Database::class, []);
    }

    public function testStringAttributeEquivalence(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            'name' => 'John Doe',
        ]);

        $legacyResult = $this->legacyProcess(clone $doc);
        $processorResult = $this->processor->processRead($this->collection, clone $doc, null, [], false);

        $this->assertEquals($legacyResult->getAttribute('name'), $processorResult->getAttribute('name'));
    }

    public function testIntegerCastingEquivalence(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            'age' => '25', // String that should be cast to int
        ]);

        $legacyResult = $this->legacyProcess(clone $doc);
        $processorResult = $this->processor->processRead($this->collection, clone $doc, null, [], false);

        $this->assertSame($legacyResult->getAttribute('age'), $processorResult->getAttribute('age'));
        $this->assertIsInt($processorResult->getAttribute('age'));
    }

    public function testBooleanCastingEquivalence(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            'active' => 1, // Int that should be cast to bool
        ]);

        $legacyResult = $this->legacyProcess(clone $doc);
        $processorResult = $this->processor->processRead($this->collection, clone $doc, null, [], false);

        $this->assertSame($legacyResult->getAttribute('active'), $processorResult->getAttribute('active'));
        $this->assertIsBool($processorResult->getAttribute('active'));
    }

    public function testJsonFilterEquivalence(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            'metadata' => '{"key":"value","nested":{"foo":"bar"}}',
        ]);

        $legacyResult = $this->legacyProcess(clone $doc);
        $processorResult = $this->processor->processRead($this->collection, clone $doc, null, [], false);

        $this->assertEquals($legacyResult->getAttribute('metadata'), $processorResult->getAttribute('metadata'));
        $this->assertIsArray($processorResult->getAttribute('metadata'));
    }

    public function testJsonArrayFilterEquivalence(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            'tags' => '["tag1","tag2","tag3"]',
        ]);

        $legacyResult = $this->legacyProcess(clone $doc);
        $processorResult = $this->processor->processRead($this->collection, clone $doc, null, [], false);

        $this->assertEquals($legacyResult->getAttribute('tags'), $processorResult->getAttribute('tags'));
        $this->assertIsArray($processorResult->getAttribute('tags'));
    }

    public function testDatetimeFilterEquivalence(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            'created_at' => '2024-01-15T10:30:00.000+00:00',
        ]);

        $legacyResult = $this->legacyProcess(clone $doc);
        $processorResult = $this->processor->processRead($this->collection, clone $doc, null, [], false);

        $this->assertEquals($legacyResult->getAttribute('created_at'), $processorResult->getAttribute('created_at'));
    }

    public function testArrayAttributeEquivalence(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            'scores' => '[1.5, 2.3, 3.7]',
        ]);

        $legacyResult = $this->legacyProcess(clone $doc);
        $processorResult = $this->processor->processRead($this->collection, clone $doc, null, [], false);

        $this->assertEquals($legacyResult->getAttribute('scores'), $processorResult->getAttribute('scores'));
        $this->assertIsArray($processorResult->getAttribute('scores'));
        foreach ($processorResult->getAttribute('scores') as $score) {
            $this->assertIsFloat($score);
        }
    }

    public function testNullValueEquivalence(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            'name' => null,
            'age' => null,
        ]);

        $legacyResult = $this->legacyProcess(clone $doc);
        $processorResult = $this->processor->processRead($this->collection, clone $doc, null, [], false);

        $this->assertNull($processorResult->getAttribute('name'));
        $this->assertNull($processorResult->getAttribute('age'));
    }

    public function testSelectionsEquivalence(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            'name' => 'John',
            'age' => 25,
            'active' => true,
        ]);

        $selections = ['name', 'age'];

        $legacyResult = $this->legacyProcess(clone $doc, $selections);
        $processorResult = $this->processor->processRead($this->collection, clone $doc, null, $selections, false);

        // Both should have selected attributes
        $this->assertEquals($legacyResult->getAttribute('name'), $processorResult->getAttribute('name'));
        $this->assertEquals($legacyResult->getAttribute('age'), $processorResult->getAttribute('age'));

        // Check if non-selected attributes are handled the same way
        $this->assertEquals(
            $legacyResult->getAttribute('active'),
            $processorResult->getAttribute('active')
        );
    }

    public function testComplexDocumentEquivalence(): void
    {
        $doc = new Document([
            '$id' => 'doc1',
            '$permissions' => ['read("any")'],
            'name' => 'Complex Doc',
            'age' => '30',
            'active' => 1,
            'tags' => '["tag1","tag2"]',
            'metadata' => '{"nested":{"deep":"value"}}',
            'created_at' => '2024-01-15T10:30:00.000+00:00',
            'scores' => '[9.5, 8.3, 7.1]',
        ]);

        $legacyResult = $this->legacyProcess(clone $doc);
        $processorResult = $this->processor->processRead($this->collection, clone $doc, null, [], false);

        // Compare all attributes
        foreach ($this->collection->getAttribute('attributes', []) as $attr) {
            $key = $attr['$id'];
            $this->assertEquals(
                $legacyResult->getAttribute($key),
                $processorResult->getAttribute($key),
                "Attribute '$key' differs between legacy and processor"
            );
        }
    }

    /**
     * Simulate legacy decode + casting process
     */
    private function legacyProcess(Document $doc, array $selections = []): Document
    {
        // Simulate casting
        foreach ($this->collection->getAttribute('attributes', []) as $attribute) {
            $key = $attribute['$id'];
            $type = $attribute['type'];
            $array = $attribute['array'] ?? false;
            $value = $doc->getAttribute($key);

            if ($value === null) {
                continue;
            }

            if ($array) {
                if (is_string($value)) {
                    $value = json_decode($value, true) ?? [];
                }
                if (!is_array($value)) {
                    $value = [$value];
                }
                foreach ($value as $i => $node) {
                    $value[$i] = $this->castValue($type, $node);
                }
            } else {
                $value = $this->castValue($type, $value);
            }

            $doc->setAttribute($key, $value);
        }

        // Simulate decode (filters)
        foreach ($this->collection->getAttribute('attributes', []) as $attribute) {
            $key = $attribute['$id'];
            $filters = $attribute['filters'] ?? [];
            $array = $attribute['array'] ?? false;
            $value = $doc->getAttribute($key);

            if (empty($filters)) {
                continue;
            }

            foreach (array_reverse($filters) as $filter) {
                if ($array && is_array($value)) {
                    foreach ($value as $i => $node) {
                        $value[$i] = $this->applyFilter($filter, $node);
                    }
                } else {
                    $value = $this->applyFilter($filter, $value);
                }
            }

            $doc->setAttribute($key, $value);
        }

        return $doc;
    }

    private function castValue(string $type, mixed $value): mixed
    {
        switch ($type) {
            case Database::VAR_STRING:
            case Database::VAR_ID:
                return (string) $value;
            case Database::VAR_INTEGER:
                return (int) $value;
            case Database::VAR_FLOAT:
                return (float) $value;
            case Database::VAR_BOOLEAN:
                return (bool) $value;
            default:
                return $value;
        }
    }

    private function applyFilter(string $filter, mixed $value): mixed
    {
        switch ($filter) {
            case 'json':
                if (!is_string($value)) {
                    return $value;
                }
                $decoded = json_decode($value, true) ?? [];
                if (array_key_exists('$id', $decoded)) {
                    return new Document($decoded);
                }
                if (is_array($decoded)) {
                    foreach ($decoded as $i => $item) {
                        if (is_array($item) && array_key_exists('$id', $item)) {
                            $decoded[$i] = new Document($item);
                        }
                    }
                }
                return $decoded;
            case 'datetime':
                return DateTime::formatTz($value);
            default:
                return $value;
        }
    }
}
