<?php

namespace Tests\Unit\Documents;

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
use Utopia\Database\Validator\Authorization;

final class FractionalBoundTest extends TestCase
{
    private const string COLLECTION = 'counters';

    private const string DOCUMENT = 'counter';

    private Database $database;

    protected function setUp(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $this->database = (new Database(new Memory(), new Cache(new None())))
            ->setAuthorization($authorization)
            ->setDatabase('fractional_bound')
            ->setNamespace('fractional_bound');
        $this->database->create();
        $this->database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::integer(key: 'count'),
                Attribute::double(key: 'ratio'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: false,
        ));
        $this->database->createDocument(self::COLLECTION, new Document([
            '$id' => self::DOCUMENT,
            'count' => 100,
            'ratio' => 1.5,
        ]));
    }

    public function testIncreaseUpToAFractionalMaximumOnAnInteger(): void
    {
        $this->assertSame(101, $this->database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 102.4)->getAttribute('count'));
        $this->assertSame(102, $this->database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 102.4)->getAttribute('count'));
        $this->assertSame(102, $this->stored('count'));
    }

    public function testIncreasePastAFractionalMaximumOnAnIntegerIsRefused(): void
    {
        $this->database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 101.0);

        try {
            $this->database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 101.9);
            $this->fail('An increase past a fractional maximum was accepted');
        } catch (LimitException $error) {
            $this->assertSame('Attribute value exceeds maximum limit: 101.9', $error->getMessage());
        }

        $this->assertSame(101, $this->stored('count'));
    }

    public function testDecreaseDownToAFractionalMinimumOnAnInteger(): void
    {
        $this->assertSame(99, $this->database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 98.5)->getAttribute('count'));
        $this->assertSame(99, $this->stored('count'));

        try {
            $this->database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 98.5);
            $this->fail('A decrease past a fractional minimum was accepted');
        } catch (LimitException $error) {
            $this->assertSame('Attribute value exceeds minimum limit: 98.5', $error->getMessage());
        }

        $this->assertSame(99, $this->stored('count'));
    }

    public function testFractionalBoundsOnADoubleStayExact(): void
    {
        $this->assertSame(2.5, $this->database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'ratio', 1, 2.5)->getAttribute('ratio'));
        $this->assertSame(2.0, $this->database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'ratio', 0.5, 2.0)->getAttribute('ratio'));
        $this->assertSame(2.0, $this->stored('ratio'));
    }

    private function stored(string $attribute): mixed
    {
        return $this->database->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute($attribute);
    }
}
