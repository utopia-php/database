<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class NumericUpdateGuardsTest extends TestCase
{
    private const string COLLECTION = 'ledgers';

    public function testUnsignedBigIntegerArithmeticNeedsTheCapability(): void
    {
        $adapter = new class () extends Memory {
            public bool $unsigned = true;

            #[\Override]
            public function supports(Capability $feature): bool
            {
                return $feature === Capability::UnsignedBigInt ? $this->unsigned : parent::supports($feature);
            }
        };
        $database = $this->database($adapter, [Attribute::bigInteger(key: 'total', signed: false)]);
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'ledger', 'total' => 5]));
        $adapter->unsigned = false;

        foreach ([
            fn (): Document => $database->increaseDocumentAttribute(self::COLLECTION, 'ledger', 'total'),
            fn (): Document => $database->decreaseDocumentAttribute(self::COLLECTION, 'ledger', 'total'),
        ] as $change) {
            $this->assertRefused(TypeException::class, 'Unsigned 64-bit arithmetic is not supported by this adapter.', $change);
        }

        $adapter->unsigned = true;
        $this->assertSame(5, $database->getDocument(self::COLLECTION, 'ledger')->getAttribute('total'));
    }

    /**
     * @return array<string, array{string, mixed, bool, class-string<\Throwable>, string}>
     */
    public static function storedValuesOutsideTheArithmetic(): array
    {
        return [
            'an integer holding a fraction' => ['count', '1.5', true, TypeException::class, 'Attribute value must be an integer.'],
            'an integer holding text' => ['count', 'abc', false, TypeException::class, 'Attribute value must be an integer.'],
            'a float holding text' => ['ratio', 'abc', true, TypeException::class, 'Attribute value must be numeric.'],
            'a float holding infinity' => ['ratio', \INF, true, TypeException::class, 'Attribute value must be a finite numeric value.'],
            'an unsigned float below zero' => ['share', -1.0, true, LimitException::class, 'Attribute value exceeds minimum limit: 0'],
        ];
    }

    /**
     * @param  class-string<\Throwable>  $exception
     */
    #[DataProvider('storedValuesOutsideTheArithmetic')]
    public function testAStoredValueTheArithmeticCannotUseIsRefused(string $attribute, mixed $stored, bool $increase, string $exception, string $message): void
    {
        $database = $this->database($this->castingItself());
        $database->skipValidation(fn (): Document => $database->createDocument(self::COLLECTION, new Document([Document::ID => 'ledger', $attribute => $stored])));

        $this->assertRefused($exception, $message, fn (): Document => $increase
            ? $database->increaseDocumentAttribute(self::COLLECTION, 'ledger', $attribute)
            : $database->decreaseDocumentAttribute(self::COLLECTION, 'ledger', $attribute));
    }

    public function testAFloatIncreasePastTheMaximumIsRefused(): void
    {
        $database = $this->database(new Memory());
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'ledger', 'ratio' => Database::MAX_DOUBLE]));

        $this->assertRefused(
            LimitException::class,
            'Attribute value exceeds maximum limit: '.Database::MAX_DOUBLE,
            fn (): Document => $database->increaseDocumentAttribute(self::COLLECTION, 'ledger', 'ratio', Database::MAX_DOUBLE),
        );
        $this->assertSame(Database::MAX_DOUBLE, $database->getDocument(self::COLLECTION, 'ledger')->getAttribute('ratio'));
    }

    public function testAFloatDecreasePastTheMinimumIsRefused(): void
    {
        $database = $this->database(new Memory());
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'ledger', 'share' => 0.5, 'ratio' => -Database::MAX_DOUBLE]));

        $this->assertRefused(
            LimitException::class,
            'Attribute value exceeds minimum limit: 0',
            fn (): Document => $database->decreaseDocumentAttribute(self::COLLECTION, 'ledger', 'share', 1),
        );
        $this->assertRefused(
            LimitException::class,
            'Attribute value exceeds minimum limit: '.(-Database::MAX_DOUBLE),
            fn (): Document => $database->decreaseDocumentAttribute(self::COLLECTION, 'ledger', 'ratio', Database::MAX_DOUBLE),
        );
        $this->assertSame(0.5, $database->getDocument(self::COLLECTION, 'ledger')->getAttribute('share'));
    }

    public function testAnIntegerStringChangeIsCheckedAsAnInteger(): void
    {
        $database = $this->database(new Memory());
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'ledger', 'count' => 10]));

        $this->assertSame(15, $database->increaseDocumentAttribute(self::COLLECTION, 'ledger', 'count', '5')->getAttribute('count'));
        $this->assertSame(12, $database->decreaseDocumentAttribute(self::COLLECTION, 'ledger', 'count', '3')->getAttribute('count'));

        foreach (['0', '-4'] as $change) {
            $this->assertRefused(\InvalidArgumentException::class, 'Value must be numeric and greater than 0', fn (): Document => $database->increaseDocumentAttribute(self::COLLECTION, 'ledger', 'count', $change));
            $this->assertRefused(\InvalidArgumentException::class, 'Value must be numeric and greater than 0', fn (): Document => $database->decreaseDocumentAttribute(self::COLLECTION, 'ledger', 'count', $change));
        }
        $this->assertSame(12, $database->getDocument(self::COLLECTION, 'ledger')->getAttribute('count'));
    }

    public function testAnUnknownAttributeOrDocumentIsNotFound(): void
    {
        $database = $this->database(new Memory());
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'ledger', 'count' => 10]));

        $this->assertRefused(NotFoundException::class, 'Attribute not found', fn (): Document => $database->increaseDocumentAttribute(self::COLLECTION, 'ledger', 'missing'));
        $this->assertRefused(NotFoundException::class, 'Attribute not found', fn (): Document => $database->decreaseDocumentAttribute(self::COLLECTION, 'ledger', 'missing'));
        $this->assertRefused(NotFoundException::class, 'Document not found', fn (): Document => $database->increaseDocumentAttribute(self::COLLECTION, 'missing', 'count'));
        $this->assertRefused(NotFoundException::class, 'Document not found', fn (): Document => $database->decreaseDocumentAttribute(self::COLLECTION, 'missing', 'count'));

        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $guarded = $this->database(new Memory(), authorization: $authorization, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: true);
        $this->assertRefused(NotFoundException::class, 'Document not found', fn (): Document => $guarded->increaseDocumentAttribute(self::COLLECTION, 'missing', 'count'));
        $this->assertRefused(NotFoundException::class, 'Document not found', fn (): Document => $guarded->decreaseDocumentAttribute(self::COLLECTION, 'missing', 'count'));
    }

    public function testIncreasingAndDecreasingNeedUpdatePermission(): void
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database = $this->database(new Memory(), authorization: $authorization, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'ledger', 'count' => 10]));

        $this->assertRefused(AuthorizationException::class, null, fn (): Document => $database->increaseDocumentAttribute(self::COLLECTION, 'ledger', 'count'));
        $this->assertRefused(AuthorizationException::class, null, fn (): Document => $database->decreaseDocumentAttribute(self::COLLECTION, 'ledger', 'count'));
        $this->assertSame(10, $database->getDocument(self::COLLECTION, 'ledger')->getAttribute('count'));
    }

    public function testASchemalessDecreaseStartsAnUnsetAttributeAtZeroAndRefusesText(): void
    {
        $database = $this->database($this->schemaless());
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'ledger', 'label' => 'text']));

        $this->assertSame(-2, $database->decreaseDocumentAttribute(self::COLLECTION, 'ledger', 'balance', 2)->getAttribute('balance'));
        $this->assertSame(-2, $database->getDocument(self::COLLECTION, 'ledger')->getAttribute('balance'));
        $this->assertRefused(TypeException::class, 'Attribute value must be numeric.', fn (): Document => $database->decreaseDocumentAttribute(self::COLLECTION, 'ledger', 'label'));
        $this->assertRefused(TypeException::class, 'Attribute value must be numeric.', fn (): Document => $database->increaseDocumentAttribute(self::COLLECTION, 'ledger', 'label'));
    }

    public function testASchemalessChangeOfADefinitionSkipsTheDocumentPermissions(): void
    {
        $database = $this->database($this->schemaless());

        $this->assertSame(1, $database->increaseDocumentAttribute(Database::METADATA, self::COLLECTION, 'revision')->getAttribute('revision'));
        $this->assertSame(0, $database->decreaseDocumentAttribute(Database::METADATA, self::COLLECTION, 'revision')->getAttribute('revision'));
    }

    private function schemaless(): Memory
    {
        return $this->without(Capability::DefinedAttributes);
    }

    private function castingItself(): Memory
    {
        return new class () extends Memory implements Feature\Casting {
            #[\Override]
            public function castBefore(Document $collection, Document $document): Document
            {
                return $document;
            }

            #[\Override]
            public function castAfter(Document $collection, array $documents): array
            {
                return $documents;
            }

            #[\Override]
            public function castDatetime(string $value): mixed
            {
                return DateTime::setTimezone($value);
            }
        };
    }

    private function without(Capability $missing): Memory
    {
        return new class ($missing) extends Memory {
            public function __construct(private readonly Capability $missing)
            {
                parent::__construct();
            }

            #[\Override]
            public function capabilities(): array
            {
                return \array_values(\array_filter(
                    parent::capabilities(),
                    fn (Capability $capability): bool => $capability !== $this->missing,
                ));
            }
        };
    }

    /**
     * @param  class-string<\Throwable>  $exception
     * @param  callable(): mixed  $change
     */
    private function assertRefused(string $exception, ?string $message, callable $change): void
    {
        $error = null;
        try {
            $change();
        } catch (\Throwable $caught) {
            $error = $caught;
        }

        $this->assertInstanceOf($exception, $error);
        if ($message !== null) {
            $this->assertSame($message, $error->getMessage());
        }
    }

    /**
     * @param  list<Attribute>|null  $attributes
     * @param  list<string>|null  $permissions
     */
    private function database(Memory $adapter, ?array $attributes = null, ?Authorization $authorization = null, ?array $permissions = null, bool $documentSecurity = false): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        if ($authorization !== null) {
            $database->setAuthorization($authorization);
        }
        $database->setDatabase('numeric')->setNamespace('numeric_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: $attributes ?? [
                Attribute::integer(key: 'count'),
                Attribute::float(key: 'ratio'),
                Attribute::float(key: 'share', signed: false),
            ],
            permissions: $permissions ?? [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: $documentSecurity,
        ));

        return $database;
    }
}
