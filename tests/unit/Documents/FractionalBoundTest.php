<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Operator;
use Utopia\Database\Validator\Authorization;

final class FractionalBoundTest extends TestCase
{
    private const string COLLECTION = 'counters';

    private const string DOCUMENT = 'counter';

    /**
     * @return iterable<string, array{bool}>
     */
    public static function lanes(): iterable
    {
        yield 'defined attributes' => [true];
        yield 'schemaless' => [false];
    }

    #[DataProvider('lanes')]
    public function testIncreaseWithAFractionalMaximumOnAnIntegerIsRefused(bool $definedAttributes): void
    {
        $database = $this->database($definedAttributes);

        try {
            $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 102.4);
            $this->fail('A fractional maximum on an integer attribute was accepted');
        } catch (TypeException $error) {
            $this->assertSame('Max must be an integer.', $error->getMessage());
        }

        $this->assertSame(100, $this->stored($database, 'count'));
    }

    #[DataProvider('lanes')]
    public function testDecreaseWithAFractionalMinimumOnAnIntegerIsRefused(bool $definedAttributes): void
    {
        $database = $this->database($definedAttributes);

        try {
            $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 0.5);
            $this->fail('A fractional minimum on an integer attribute was accepted');
        } catch (TypeException $error) {
            $this->assertSame('Min must be an integer.', $error->getMessage());
        }

        $this->assertSame(100, $this->stored($database, 'count'));
    }

    #[DataProvider('lanes')]
    public function testANonNumericBoundOnAnIntegerIsRefused(bool $definedAttributes): void
    {
        $database = $this->database($definedAttributes);

        $this->expectException(TypeException::class);
        $this->expectExceptionMessage('Max must be an integer.');

        $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, '102.5');
    }

    #[DataProvider('lanes')]
    public function testWholeFloatBoundsOnAnIntegerAreComparedExactly(bool $definedAttributes): void
    {
        $database = $this->database($definedAttributes);

        $this->assertSame(101, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 101.0)->getAttribute('count'));

        try {
            $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 101.0);
            $this->fail('An increase past a whole float maximum was accepted');
        } catch (LimitException $error) {
            $this->assertSame('Attribute value exceeds maximum limit: 101', $error->getMessage());
        }

        $this->assertSame(102, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 9.0e18)->getAttribute('count'));
        $this->assertSame(101, $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, -9.0e18)->getAttribute('count'));
        $this->assertSame(102, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, 1.0e19)->getAttribute('count'));
        $this->assertSame(101, $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, -1.0e19)->getAttribute('count'));
        $this->assertSame(101, $this->stored($database, 'count'));
    }

    #[DataProvider('lanes')]
    public function testWholeNumberStringBoundsOnAnIntegerAreAcceptedAsOperatorLimitsAre(bool $definedAttributes): void
    {
        $database = $this->database($definedAttributes);

        $this->assertSame(101, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, '101.0')->getAttribute('count'));

        try {
            $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, '101.00');
            $this->fail('An increase past a whole number string maximum was accepted');
        } catch (LimitException $error) {
            $this->assertSame('Attribute value exceeds maximum limit: 101', $error->getMessage());
        }

        $this->assertSame(100, $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1, '-5.0')->getAttribute('count'));
        $this->assertSame(101, $database->updateDocument(self::COLLECTION, self::DOCUMENT, new Document([
            'count' => Operator::increment(1, '101.0'),
        ]))->getAttribute('count'));
        $this->assertSame(101, $this->stored($database, 'count'));
    }

    #[DataProvider('lanes')]
    public function testAFractionalChangeValueOnAnIntegerIsRefused(bool $definedAttributes): void
    {
        $database = $this->database($definedAttributes);

        foreach ([
            'increase' => static fn (): Document => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 1.5),
            'decrease' => static fn (): Document => $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 0.5),
            'increase by a numeric string' => static fn (): Document => $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', '1.5'),
        ] as $case => $change) {
            try {
                $change();
                $this->fail("A fractional change value on an integer attribute was accepted ({$case})");
            } catch (TypeException $error) {
                $this->assertSame('Change value must be an integer.', $error->getMessage(), $case);
            }
        }

        $this->assertSame(100, $this->stored($database, 'count'));
        $this->assertSame(102, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'count', 2)->getAttribute('count'));
        $this->assertSame(3.0, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'ratio', 1.5)->getAttribute('ratio'));
    }

    #[DataProvider('lanes')]
    public function testFractionalBoundsOnADoubleAreAccepted(bool $definedAttributes): void
    {
        $database = $this->database($definedAttributes);

        $this->assertSame(2.5, $database->increaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'ratio', 1, 2.5)->getAttribute('ratio'));
        $this->assertSame(2.0, $database->decreaseDocumentAttribute(self::COLLECTION, self::DOCUMENT, 'ratio', 0.5, 2.0)->getAttribute('ratio'));
        $this->assertSame(2.0, $this->stored($database, 'ratio'));
    }

    private function database(bool $definedAttributes): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $adapter = $definedAttributes ? new Memory() : new class () extends Memory {
            public function capabilities(): array
            {
                return \array_values(\array_filter(
                    parent::capabilities(),
                    static fn (Capability $capability): bool => $capability !== Capability::DefinedAttributes,
                ));
            }
        };

        $database = (new Database($adapter, new Cache(new None())))
            ->setAuthorization($authorization)
            ->setDatabase('fractional_bound')
            ->setNamespace('fractional_bound');
        $database->create();
        $database->createCollection(new Collection(
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
        $database->createDocument(self::COLLECTION, new Document([
            '$id' => self::DOCUMENT,
            'count' => 100,
            'ratio' => 1.5,
        ]));

        return $database;
    }

    private function stored(Database $database, string $attribute): mixed
    {
        return $database->getDocument(self::COLLECTION, self::DOCUMENT)->getAttribute($attribute);
    }
}
