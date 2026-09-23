<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

final class NullNumericIncrementTest extends TestCase
{
    private const string COLLECTION = 'counters';

    private const string DOCUMENT = 'unset';

    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new Memory(), new Cache(new None()));
        $this->database
            ->setDatabase('null_increment')
            ->setNamespace('null_increment_'.\uniqid());
        $this->database->getAuthorization()->addRole(Role::any()->toString());
        $this->database->create();
        $this->database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::integer('integer', required: false),
                Attribute::bigInteger('bigInteger', required: false),
                Attribute::float('float', required: false),
                Attribute::double('double', required: false),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ));
        $this->database->createDocument(self::COLLECTION, new Document(['$id' => self::DOCUMENT]));
    }

    /**
     * @return array<string, array{string, int|float, int|float}>
     */
    public static function increments(): array
    {
        return [
            'integer' => ['integer', 3, 3],
            'bigInteger' => ['bigInteger', 4, 4],
            'float' => ['float', 1.5, 1.5],
            'double' => ['double', 2.25, 2.25],
        ];
    }

    /**
     * @return array<string, array{string, int|float, int|float}>
     */
    public static function decrements(): array
    {
        return [
            'integer' => ['integer', 2, -2],
            'bigInteger' => ['bigInteger', 5, -5],
            'float' => ['float', 0.5, -0.5],
            'double' => ['double', 1.25, -1.25],
        ];
    }

    #[DataProvider('increments')]
    public function testIncreaseTreatsAnUnsetOptionalNumberAsZero(string $attribute, int|float $value, int|float $expected): void
    {
        $this->assertNull($this->database->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute($attribute));

        $increased = $this->database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, $attribute, $value);

        $this->assertSame($expected, $increased->getAttribute($attribute));
        $this->assertSame($expected, $this->database->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute($attribute));
    }

    #[DataProvider('decrements')]
    public function testDecreaseTreatsAnUnsetOptionalNumberAsZero(string $attribute, int|float $value, int|float $expected): void
    {
        $decreased = $this->database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, $attribute, $value);

        $this->assertSame($expected, $decreased->getAttribute($attribute));
        $this->assertSame($expected, $this->database->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute($attribute));
    }

    public function testIncreaseOfAnUnsetNumberStillEnforcesTheMaximum(): void
    {
        $this->expectException(LimitException::class);

        $this->database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'integer', 5, max: 4);
    }

    public function testDecreaseOfAnUnsetNumberStillEnforcesTheMinimum(): void
    {
        $this->expectException(LimitException::class);

        $this->database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'float', 2.5, min: -2);
    }

    public function testIncreaseOfAnUnsetNumberWithinTheMaximumSucceeds(): void
    {
        $increased = $this->database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'integer', 4, max: 4);

        $this->assertSame(4, $increased->getAttribute('integer'));
    }
}
