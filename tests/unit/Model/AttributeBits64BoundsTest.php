<?php

namespace Tests\Unit\Model;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\IntegerWidth;
use Utopia\Database\Validator\Authorization;

/**
 * A 64-bit integer column holds any PHP integer, so its bounds are the 64-bit ones the structure validator applies
 * to an integer of size 8, not the 32-bit ones.
 */
final class AttributeBits64BoundsTest extends TestCase
{
    private const string COLLECTION = 'counters';

    private const string COUNTER = 'total';

    /**
     * @return array<string, array{Attribute, int, int}>
     */
    public static function bounds(): array
    {
        return [
            'signed 64-bit' => [Attribute::integer('a', width: IntegerWidth::Bits64), \PHP_INT_MIN, Database::MAX_BIG_INT],
            'unsigned 64-bit' => [Attribute::integer('a', signed: false, width: IntegerWidth::Bits64), 0, Database::MAX_BIG_INT],
            'signed 32-bit' => [Attribute::integer('a'), Database::MIN_INT, Database::MAX_INT],
            'unsigned 32-bit' => [Attribute::integer('a', signed: false), 0, Database::MAX_INT],
            'stored size 8' => [Attribute::fromArray(['key' => 'a', 'type' => 'integer', 'size' => 8, 'signed' => true]), \PHP_INT_MIN, Database::MAX_BIG_INT],
        ];
    }

    #[DataProvider('bounds')]
    public function testBounds(Attribute $attribute, int $min, int $max): void
    {
        $bounds = $attribute->bounds();

        $this->assertNotNull($bounds);
        $this->assertSame($min, $bounds->min);
        $this->assertSame($max, $bounds->max);
    }

    public function testIncreasingPastThe32BitMaximumSucceeds(): void
    {
        $database = $this->database(signed: true);

        $document = $database->increaseDocumentAttribute(self::COLLECTION, 'counter', self::COUNTER, 1);

        $this->assertSame(Database::MAX_INT + 1, $document->getAttribute(self::COUNTER));
        $this->assertSame(Database::MAX_INT + 1, $database->getDocument(self::COLLECTION, 'counter')->getAttribute(self::COUNTER));
    }

    public function testIncreasingPastThe64BitMaximumFails(): void
    {
        $database = $this->database(signed: false);
        $database->updateDocument(self::COLLECTION, 'counter', new Document([self::COUNTER => Database::MAX_BIG_INT]));

        $this->expectException(Limit::class);

        $database->increaseDocumentAttribute(self::COLLECTION, 'counter', self::COUNTER, 1);
    }

    public function testDecreasingAnUnsignedCounterBelowZeroFails(): void
    {
        $database = $this->database(signed: false);
        $database->updateDocument(self::COLLECTION, 'counter', new Document([self::COUNTER => 0]));

        $this->expectException(Limit::class);

        $database->decreaseDocumentAttribute(self::COLLECTION, 'counter', self::COUNTER, 1);
    }

    private function database(bool $signed): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setDatabase('bounds')
            ->setNamespace('bounds_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::integer(key: self::COUNTER, signed: $signed, width: IntegerWidth::Bits64)],
            permissions: [Permission::read(Role::any()), Permission::update(Role::any()), Permission::create(Role::any())],
        ));
        $database->createDocument(self::COLLECTION, new Document([
            Document::ID => 'counter',
            self::COUNTER => Database::MAX_INT,
        ]));

        return $database;
    }
}
