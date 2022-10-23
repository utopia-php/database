<?php

namespace Utopia\Tests;

use PHPUnit\Framework\TestCase;
use Utopia\CLI\Console;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\ID;
use Utopia\Database\Validator\Authorization;

abstract class Base extends TestCase
{
    /**
     * @return Database
     */
    abstract static protected function getDatabase(): Database;

    /**
     * @return string
     */
    abstract static protected function getAdapterName(): string;

    /**
     * @return int
     */
    abstract static protected function getAdapterRowLimit(): int;

    static public array $collections;

    public function setUp(): void
    {
        Authorization::setRole('any');
    }

    public function tearDown(): void
    {
        Authorization::reset();
    }

    protected string $testDatabase = 'utopiaTests';

    public function testCreateExistsDelete()
    {
        $this->assertEquals(true, static::getDatabase()->create($this->testDatabase));
        self::$collections = include __DIR__ . '/collections.php';
    }


    public function atestCreatedAtUpdatedAt()
    {
        Console::error('option 1');
        $start = microtime(true);
        Console::error('start creating collections: ' . $start);

        foreach (self::$collections as $key => $collection) {
            if (($collection['$collection'] ?? '') !== Database::METADATA) {
                continue;
            }

            $attributes = [];
            $indexes = [];

            foreach ($collection['attributes'] as $attribute) {
                $attributes[] = new Document([
                    '$id' => ID::custom($attribute['$id']),
                    'type' => $attribute['type'],
                    'size' => $attribute['size'],
                    'required' => $attribute['required'],
                    'signed' => $attribute['signed'],
                    'array' => $attribute['array'],
                    'filters' => $attribute['filters'],
                    'default' => $attribute['default'] ?? null,
                    'format' => $attribute['format'] ?? ''
                ]);
            }

            foreach ($collection['indexes'] as $index) {
                $indexes[] = new Document([
                    '$id' => ID::custom($index['$id']),
                    'type' => $index['type'],
                    'attributes' => $index['attributes'],
                    'lengths' => $index['lengths'],
                    'orders' => $index['orders'],
                ]);
            }

            static::getDatabase()->createCollection($key, $attributes, $indexes);
            Console::success('collection: ' . $collection['$id'] . ' created');
        }

        Console::error('end: ' . microtime(true) - $start);
    }



    public function testCreatedAtUpdatedAt()
    {

        static::getDatabase()->setNamespace('myapp_'.uniqid());
        $this->assertEquals(true, static::getDatabase()->create($this->testDatabase));

        Console::error('option 2');
        $start = microtime(true);
        Console::error('start creating collections: ' . $start);

        foreach (self::$collections as $key => $collection) {
            if (($collection['$collection'] ?? '') !== Database::METADATA) {
                continue;
            }

            static::getDatabase()->createCollection($key);

            foreach ($collection['attributes'] as $attribute) {
                //$attribute['default'] = $attribute['default'] ?? null;
                $attribute['default'] = null;
                $attribute['filters'] = $attribute['filters'] ?? [];
                $attribute['format'] = $attribute['format'] ?? '';
                static::getDatabase()->createAttribute(
                    $key,
                    $attribute['$id'],
                    $attribute['type'],
                    $attribute['size'],
                    $attribute['required'],
                    $attribute['default'],
                    $attribute['signed'],
                    $attribute['array'],
                    $attribute['format'],
                    [],
                    $attribute['filters']
                );
            }

            foreach ($collection['indexes'] as $index) {
                static::getDatabase()->createIndex(
                    $key,
                    $index['$id'],
                    $index['type'],
                    $index['attributes'],
                    $index['lengths'],
                    $index['orders'],
                );
            }

            Console::success('collection: ' . $collection['$id'] . ' created');
        }

        Console::error('end: ' . microtime(true) - $start);
    }


}
