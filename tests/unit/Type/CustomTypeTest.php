<?php

namespace Tests\Unit\Type;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Type\Custom;
use Utopia\Database\Type\TypeRegistry;
use Utopia\Query\Schema\ColumnType;

final class CustomTypeTest extends TestCase
{
    public function testRegisteredTypeEncodesAndDecodesOnItsHandle(): void
    {
        $adapter = new Memory();
        $registry = new TypeRegistry();
        $database = $this->database($adapter)->setTypeRegistry($registry);
        $registry->register(new Reversed());

        $this->createNote($database, 'hello', ['reversed']);

        $this->assertSame('olleh', $adapter->getDocument($database->getCollection('notes'), 'note')->getAttribute('body'));
        $this->assertSame('hello', $database->getDocument('notes', 'note')->getAttribute('body'));
    }

    public function testHandlesWithoutTheRegistryCannotEncodeItsTypes(): void
    {
        $this->database()->setTypeRegistry($this->registry(new Reversed()));

        $this->expectException(NotFoundException::class);
        $this->database()->encode($this->notes(), new Document(['body' => 'hello']));
    }

    public function testHandlesWithoutTheRegistryCannotDecodeItsTypes(): void
    {
        $this->database()->setTypeRegistry($this->registry(new Reversed()));

        $this->expectException(NotFoundException::class);
        $this->database()->decode($this->notes(), new Document(['body' => 'olleh']));
    }

    public function testABuiltInNameCannotBreakOtherHandles(): void
    {
        $adapter = new Memory();
        $this->createNote($this->database($adapter), 'hello');

        $registry = new TypeRegistry();
        $this->database()->setTypeRegistry($registry);

        try {
            $registry->register(new Reversed('json'));
        } catch (DuplicateException) {
        }

        $this->assertSame('hello', $this->database($adapter)->getDocument('notes', 'note')->getAttribute('body'));
    }

    public function testARegisteredTypeShadowsAGlobalFilterOnlyOnItsHandle(): void
    {
        $database = $this->database()->setTypeRegistry($this->registry(new Reversed()));
        $other = $this->database();

        $filters = new \ReflectionProperty(Database::class, 'filters');
        $previous = $filters->getValue();

        try {
            Database::addFilter(
                'reversed',
                static fn (mixed $value): mixed => $value,
                static fn (mixed $value): string => 'global',
            );

            $this->assertSame('hello', $database->decode($this->notes(), new Document(['body' => 'olleh']))->getAttribute('body'));
            $this->assertSame('global', $other->decode($this->notes(), new Document(['body' => 'olleh']))->getAttribute('body'));
        } finally {
            $filters->setValue(null, $previous);
        }
    }

    public function testRegisteringATypeChangesOnlyItsHandlesCacheKeys(): void
    {
        $plain = $this->database();
        $before = $this->documentHash($plain);

        $typed = $this->database()->setTypeRegistry($this->registry(new Reversed()));

        $this->assertNotSame($before, $this->documentHash($typed));
        $this->assertSame($before, $this->documentHash($plain));
    }

    public function testCacheKeysFollowTheTypeClass(): void
    {
        $reversed = $this->database()->setTypeRegistry($this->registry(new Reversed('text')));
        $rot13 = $this->database()->setTypeRegistry($this->registry(new Rot13('text')));
        $sameClass = $this->database()->setTypeRegistry($this->registry(new Reversed('text')));

        $this->assertNotSame($this->documentHash($reversed), $this->documentHash($rot13));
        $this->assertNotSame(
            $reversed->getQueryCacheField(null, [Query::limit(1)]),
            $rot13->getQueryCacheField(null, [Query::limit(1)]),
        );
        $this->assertSame($this->documentHash($reversed), $this->documentHash($sameClass));
    }

    public function testConstructorFiltersTakePrecedenceOverRegisteredTypes(): void
    {
        $identity = static fn (mixed $value): mixed => $value;
        $filters = ['reversed' => ['encode' => $identity, 'decode' => $identity]];

        $database = $this->database(filters: $filters)->setTypeRegistry($this->registry(new Reversed()));
        $constructorOnly = $this->database(filters: $filters);

        $this->assertSame('olleh', $database->decode($this->notes(), new Document(['body' => 'olleh']))->getAttribute('body'));
        $this->assertSame($this->documentHash($constructorOnly), $this->documentHash($database));
    }

    /**
     * @param  array<string, array{encode: callable, decode: callable}>  $filters
     */
    private function database(?Memory $adapter = null, array $filters = []): Database
    {
        return (new Database($adapter ?? new Memory(), new Cache(new None()), $filters))
            ->setDatabase('types')
            ->setNamespace('types');
    }

    private function registry(Custom $type): TypeRegistry
    {
        $registry = new TypeRegistry();
        $registry->register($type);

        return $registry;
    }

    private function notes(): Document
    {
        return new Document([
            '$id' => 'notes',
            'attributes' => [
                new Document([
                    '$id' => 'body',
                    'type' => ColumnType::String->value,
                    'array' => false,
                    'filters' => ['reversed'],
                ]),
            ],
        ]);
    }

    /**
     * @param  array<string>  $filters
     */
    private function createNote(Database $database, string $body, array $filters = []): void
    {
        $database->create();
        $database->createCollection(new Collection(id: 'notes'));
        $database->createAttribute('notes', Attribute::string(key: 'body', filters: $filters));
        $database->createDocument('notes', new Document([
            '$id' => 'note',
            '$permissions' => [Permission::read(Role::any())],
            'body' => $body,
        ]));
    }

    private function documentHash(Database $database): string
    {
        return $database->getCacheKeys('notes', 'note')[2];
    }
}
